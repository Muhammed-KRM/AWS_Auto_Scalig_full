using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace WorkerAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class WorkerController : ControllerBase
    {
        // POST api/worker/computeTask
        [HttpPost("computeTask")]
        public IActionResult ComputeTask([FromBody] TaskData taskData)
        {
            if (taskData == null || taskData.ArrayA.Length != taskData.ArrayB.Length)
            {
                return BadRequest("Invalid task data");
            }

            // Perform the computation on the API
            var resultArray = new List<byte>();
            for (int i = 0; i < taskData.ArrayA.Length; i++)
            {
                int result = 1;
                for (int j = 0; j < taskData.ArrayB[i]; j++)
                {
                    result *= (taskData.ArrayA[i] ^ 10) * (taskData.ArrayB[i] ^ 10) +
                              (taskData.ArrayB[i] / (taskData.ArrayA[i] + 1)) +
                              (int)(Math.Sin(taskData.ArrayA[i]) * 1000);
                }

                // Store result limited to a byte range
                resultArray.Add((byte)(result & 0x000000FF));
            }

            Console.WriteLine($"Task {taskData.TaskIndex} computed successfully.");

            // Return the computed results to the client
            return Ok(new { TaskIndex = taskData.TaskIndex, Results = resultArray });
        }
    }

    // Model to represent the data sent from EC2
    public class TaskData
    {
        public int TaskIndex { get; set; }
        public int[] ArrayA { get; set; }
        public int[] ArrayB { get; set; }
    }
}