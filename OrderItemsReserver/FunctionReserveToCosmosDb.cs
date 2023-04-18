using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.Azure.Cosmos;

namespace OrderItemsReserver
{
    public static class FunctionReserveToCosmosDb
    {
        [FunctionName("ReserveToCosmosDb")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();

            if (requestBody == null)
            {
                return new OkObjectResult("This HTTP triggered function failed.");
            }

            dynamic data = JsonConvert.DeserializeObject(requestBody);

            // Cosmos Db section

            // Replace <documentEndpoint> with the information created earlier
            string EndpointUri = Environment.GetEnvironmentVariable("CosmosDbUri", EnvironmentVariableTarget.Process); ;

            // Set variable to the Primary Key from earlier.
            string PrimaryKey = Environment.GetEnvironmentVariable("CosmosDbPrimaryKey", EnvironmentVariableTarget.Process);

            // The names of the database and container we will create
            string databaseId = "eShopOnWeb";
            string containerId = "Orders";

            // The Cosmos client instance
            CosmosClient cosmosClient = new CosmosClient(EndpointUri, PrimaryKey);

            // The database we will create
            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);

            // The container we will create.
            Container container = await database.CreateContainerIfNotExistsAsync(containerId, "/OrderSummary");

            // Container write out
            var response = await container.CreateItemAsync(data);

            string responseMessage = string.IsNullOrEmpty(response.ToString())
                ? "This HTTP triggered function failed."
                : $"This HTTP triggered function executed successfully. {response.ToString()}";

            return new OkObjectResult(responseMessage);
        }
    }
}
