using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Azure.Storage.Blobs;

namespace OrderItemsReserver
{
	public static class Function1
    {
        [FunctionName("Function1")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
			log.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

			string storageConfig = Environment.GetEnvironmentVariable("BlobConnectionString", EnvironmentVariableTarget.Process);
			string blobContainerName = Environment.GetEnvironmentVariable("BlobContainerName", EnvironmentVariableTarget.Process);

			BlobServiceClient blobServiceClient = new BlobServiceClient(storageConfig);

			// Get the container (folder) the file will be saved in
			BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(blobContainerName);

			string blobName = blobContainerName + DateTime.UtcNow.ToShortTimeString();

			// Get the Blob Client used to interact with (including create) the blob
			BlobClient blobClient = containerClient.GetBlobClient(blobName);

			using (MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(requestBody)))
			{
				await blobClient.UploadAsync(ms);
			}

            string responseMessage = string.IsNullOrEmpty(requestBody)
                ? "This HTTP triggered function failed."
                : $"This HTTP triggered function executed successfully. {requestBody}";

			return new OkObjectResult(responseMessage);
        }
	}
}
