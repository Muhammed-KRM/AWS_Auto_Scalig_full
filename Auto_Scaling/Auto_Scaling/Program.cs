using Amazon;
using Amazon.EC2;
using Amazon.EC2.Model;
using Amazon.Runtime;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

class Program
{
    private static AmazonEC2Client ec2Client = null!;
    private static AmazonSimpleSystemsManagementClient ssmClient = null!;
    private const int NumberOfWork = 5000000;
    private static int TaskCount = 8;
    private static readonly Random random = new Random();
    private static List<string> instancePublicIPs = new List<string>();
    private static readonly HttpClient httpClient = new HttpClient() { Timeout = TimeSpan.FromSeconds(30) };

    static public int[] a = null!;
    static public int[] b = null!;
    public static bool allTasksCompleted;
    public static int batchSize = 1000;

    private static Dictionary<string, long> timings = new Dictionary<string, long>();

    static async Task Main(string[] args)
    {
        var totalStopwatch = Stopwatch.StartNew();
        var currentStopwatch = new Stopwatch();

        try
        {
            // AWS Credentials
            currentStopwatch.Start();
            string awsAccessKey = Environment.GetEnvironmentVariable("AWS_ACCESS_KEY") ?? "YOUR_AWS_ACCESS_KEY";
            string awsSecretKey = Environment.GetEnvironmentVariable("AWS_SECRET_KEY") ?? "YOUR_AWS_SECRET_KEY";

            var credentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
            ec2Client = new AmazonEC2Client(credentials, RegionEndpoint.EUCentral1);
            ssmClient = new AmazonSimpleSystemsManagementClient(credentials, RegionEndpoint.EUCentral1);
            currentStopwatch.Stop();
            timings["AWS Configuration"] = currentStopwatch.ElapsedMilliseconds;

            // EC2 Instances Start
            Console.WriteLine("\nStarting EC2 instances...");
            currentStopwatch.Restart();
            var instanceIds = await StartInstances(TaskCount);
            currentStopwatch.Stop();
            timings["Starting EC2 Instances"] = currentStopwatch.ElapsedMilliseconds;

            // Instance Wait
            Console.WriteLine("\nWaiting for instances to be ready...");
            currentStopwatch.Restart();
            await WaitForInstancesRunning(instanceIds);
            currentStopwatch.Stop();
            timings["Waiting for Instances"] = currentStopwatch.ElapsedMilliseconds;

            // Get IP Addresses
            Console.WriteLine("\nGetting IP addresses...");
            currentStopwatch.Restart();
            instancePublicIPs = await GetPublicIPAddresses(instanceIds);
            currentStopwatch.Stop();
            timings["Getting IP Addresses"] = currentStopwatch.ElapsedMilliseconds;

            if (instancePublicIPs.Count < TaskCount)
            {
                throw new Exception("Not all instances have public IPs assigned.");
            }

            // Prepare Arrays
            Console.WriteLine("\nPreparing work arrays...");
            currentStopwatch.Restart();
            PrepareWorkArrays();
            currentStopwatch.Stop();
            timings["Preparing Arrays"] = currentStopwatch.ElapsedMilliseconds;

            // Process Tasks
            Console.WriteLine("\nProcessing tasks...");
            currentStopwatch.Restart();
            try
            {
                var tasks = new List<Task>();
                for (int i = 0; i < TaskCount; i++)
                {
                    int taskIndex = i;
                    tasks.Add(ProcessTaskWithRetry(taskIndex));
                }

                await Task.WhenAll(tasks);
            }
            finally
            {
                currentStopwatch.Stop();
                timings["Processing Tasks"] = currentStopwatch.ElapsedMilliseconds;
            }

            // Cleanup
            Console.WriteLine("\nCleaning up...");
            currentStopwatch.Restart();
            await CleanupInstances(instanceIds);
            currentStopwatch.Stop();
            timings["Cleanup"] = currentStopwatch.ElapsedMilliseconds;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"\nFatal error: {ex.Message}");
            Console.WriteLine(ex.StackTrace);
        }
        finally
        {
            totalStopwatch.Stop();
            timings["Total Execution"] = totalStopwatch.ElapsedMilliseconds;

            // Zamanlamaları göster
            Console.WriteLine("\n=== Execution Times ===");
            Console.WriteLine("----------------------");
            foreach (var timing in timings.OrderBy(t => t.Key))
            {
                Console.WriteLine($"{timing.Key,-25}: {timing.Value,8:N0} ms ({TimeSpan.FromMilliseconds(timing.Value).TotalSeconds,6:N2} seconds)");
            }
            Console.WriteLine("----------------------");

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }

    private static void PrepareWorkArrays()
    {
        a = new int[NumberOfWork];
        b = new int[NumberOfWork];
        for (int i = 0; i < NumberOfWork; i++)
        {
            a[i] = random.Next(1, 11);
            b[i] = random.Next(1000, 2001);
        }
    }

    private static async Task<List<string>> StartInstances(int instanceCount)
    {
        var runInstancesRequest = new RunInstancesRequest
        {
            ImageId = "ami-0c1d9dfbfa2c2ced5",
            InstanceType = InstanceType.T2Micro,
            MinCount = instanceCount,
            MaxCount = instanceCount,
            KeyName = "Yourkey",
            SecurityGroupIds = new List<string> { "sg-071a84c40b521775b" },
            UserData = Convert.ToBase64String(Encoding.UTF8.GetBytes(@"#!/bin/bash
                # API başlatma komutları buraya eklenebilir"))
        };

        var runResponse = await ec2Client.RunInstancesAsync(runInstancesRequest);
        var instanceIds = new List<string>();

        foreach (var instance in runResponse.Reservation.Instances)
        {
            instanceIds.Add(instance.InstanceId);
            Console.WriteLine($"Instance {instance.InstanceId} started.");
        }

        return instanceIds;
    }

    private static async Task WaitForInstancesRunning(List<string> instanceIds)
    {
        Console.WriteLine("Waiting for instances to be ready...");
        bool allRunning = false;
        int maxAttempts = 30;
        int attempt = 0;
        var waitStopwatch = Stopwatch.StartNew();

        while (!allRunning && attempt < maxAttempts)
        {
            var status = await GetInstanceStatus(instanceIds);
            allRunning = status.All(s => s.Value == InstanceStateName.Running);

            if (!allRunning)
            {
                Console.WriteLine($"Waiting for instances to be ready... Attempt {attempt + 1}/{maxAttempts} " +
                                $"(Elapsed: {waitStopwatch.ElapsedMilliseconds / 1000.0:F1} seconds)");
                await Task.Delay(10000);
                attempt++;
            }
        }

        waitStopwatch.Stop();
        if (!allRunning)
        {
            throw new TimeoutException($"Instances failed to reach running state in {waitStopwatch.ElapsedMilliseconds / 1000.0:F1} seconds");
        }

        Console.WriteLine($"All instances are running after {waitStopwatch.ElapsedMilliseconds / 1000.0:F1} seconds. Waiting for API initialization...");
        await Task.Delay(60000);
    }

    private static async Task<Dictionary<string, InstanceStateName>> GetInstanceStatus(List<string> instanceIds)
    {
        var request = new DescribeInstancesRequest { InstanceIds = instanceIds };
        var response = await ec2Client.DescribeInstancesAsync(request);

        return response.Reservations
            .SelectMany(r => r.Instances)
            .ToDictionary(i => i.InstanceId, i => i.State.Name);
    }

    private static async Task<List<string>> GetPublicIPAddresses(List<string> instanceIds)
    {
        var describeInstancesRequest = new DescribeInstancesRequest { InstanceIds = instanceIds };
        var describeInstancesResponse = await ec2Client.DescribeInstancesAsync(describeInstancesRequest);

        var ipAddresses = describeInstancesResponse.Reservations
            .SelectMany(r => r.Instances)
            .Select(i => i.PublicIpAddress)
            .Where(ip => !string.IsNullOrEmpty(ip))
            .ToList();

        Console.WriteLine("\nRetrieved IP addresses:");
        foreach (var ip in ipAddresses)
        {
            Console.WriteLine($"- {ip}");
        }

        return ipAddresses;
    }

    private static async Task ProcessTaskWithRetry(int taskIndex)
    {
        var taskStopwatch = Stopwatch.StartNew();
        int totalJobs = NumberOfWork / TaskCount;
        int startIndex = taskIndex * totalJobs;
        int endIndex = (taskIndex + 1) * totalJobs;
        int completedBatches = 0;
        int totalBatches = (endIndex - startIndex) / batchSize + ((endIndex - startIndex) % batchSize > 0 ? 1 : 0);

        try
        {
            for (int i = startIndex; i < endIndex; i += batchSize)
            {
                int batchEnd = Math.Min(i + batchSize, endIndex);
                await SendBatchWithRetry(taskIndex, i, batchEnd);
                completedBatches++;

                if (completedBatches % 5 == 0)  // Her 5 batch'te bir ilerleme raporu
                {
                    Console.WriteLine($"Task {taskIndex}: Completed {completedBatches}/{totalBatches} batches " +
                                    $"({(completedBatches * 100.0 / totalBatches):F1}%) in {taskStopwatch.ElapsedMilliseconds / 1000.0:F1} seconds");
                }
            }
        }
        finally
        {
            taskStopwatch.Stop();
            Console.WriteLine($"\nTask {taskIndex} completed in {taskStopwatch.ElapsedMilliseconds:N0} ms " +
                            $"({taskStopwatch.ElapsedMilliseconds / 1000.0:F1} seconds)");
        }
    }

    private static async Task SendBatchWithRetry(int taskIndex, int start, int end)
    {
        const int maxRetries = 3;
        var batchStopwatch = Stopwatch.StartNew();

        for (int retry = 0; retry < maxRetries; retry++)
        {
            try
            {
                var data = new
                {
                    TaskIndex = taskIndex,
                    ArrayA = a.Skip(start).Take(end - start).ToArray(),
                    ArrayB = b.Skip(start).Take(end - start).ToArray()
                };

                var jsonContent = new StringContent(
                    JsonSerializer.Serialize(data),
                    Encoding.UTF8,
                    "application/json");

                var response = await httpClient.PostAsync(
                    $"http://{instancePublicIPs[taskIndex]}:5039/api/Worker/computeTask",
                    jsonContent);

                if (response.IsSuccessStatusCode)
                {
                    batchStopwatch.Stop();
                    Console.WriteLine($"Task {taskIndex}: Batch {start}-{end} completed in {batchStopwatch.ElapsedMilliseconds} ms");
                    return;
                }

                string errorResponse = await response.Content.ReadAsStringAsync();
                Console.WriteLine($"Task {taskIndex}: Attempt {retry + 1} failed with status {response.StatusCode}. Error: {errorResponse}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Task {taskIndex}: Attempt {retry + 1} failed with error: {ex.Message}");
                if (retry == maxRetries - 1) throw;
            }

            await Task.Delay(2000 * (retry + 1));
        }
    }

    private static async Task CleanupInstances(List<string> instanceIds)
    {
        Console.WriteLine("Terminating instances...");
        var cleanupStopwatch = Stopwatch.StartNew();

        try
        {
            var runningInstances = await GetRunningInstances();
            if (runningInstances.Any())
            {
                Console.WriteLine("Terminating running instances:");
                foreach (var instanceId in runningInstances)
                {
                    Console.WriteLine($"- {instanceId}");
                }

                var terminateRequest = new TerminateInstancesRequest { InstanceIds = runningInstances };
                var terminateResponse = await ec2Client.TerminateInstancesAsync(terminateRequest);

                foreach (var instance in terminateResponse.TerminatingInstances)
                {
                    Console.WriteLine($"Instance {instance.InstanceId} is terminating. Current state: {instance.CurrentState.Name}");
                }
            }
            else
            {
                Console.WriteLine("No running instances to terminate.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error during cleanup: {ex.Message}");
        }
        finally
        {
            cleanupStopwatch.Stop();
            Console.WriteLine($"Cleanup completed in {cleanupStopwatch.ElapsedMilliseconds:N0} ms");
        }
    }

    private static async Task<List<string>> GetRunningInstances()
    {
        var describeInstancesRequest = new DescribeInstancesRequest();
        var describeInstancesResponse = await ec2Client.DescribeInstancesAsync(describeInstancesRequest);

        return describeInstancesResponse.Reservations
            .SelectMany(r => r.Instances)
            .Where(i => i.State.Name == InstanceStateName.Running)
            .Select(i => i.InstanceId)
            .ToList();
    }
}