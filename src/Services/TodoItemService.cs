using todo.Models;

namespace todo
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;

    public static class TodoItemService
    {
        private static readonly string DatabaseId = ConfigurationManager.AppSettings["database"] ?? "Tasks";
        private static readonly string ContainerId = ConfigurationManager.AppSettings["container"] ?? "Items";
        private static readonly string Endpoint = ConfigurationManager.AppSettings["endpoint"];
        private static readonly string PrimaryKey = ConfigurationManager.AppSettings["primaryKey"];
        private static CosmosItems items;
        private static CosmosClient client;

        public static async Task<TodoItem> GetTodoItemAsync(string id, string partitionKey)
        {
            TodoItem item = await items.ReadItemAsync<TodoItem>(partitionKey, id);
            return item;
        }

        public static async Task<IEnumerable<TodoItem>> GetOpenItemsAsync()
        {
            var queryText = "SELECT* FROM c WHERE c.isComplete != true";
            var querySpec = new CosmosSqlQueryDefinition(queryText);

            // Selecting all tasks that are not completed is a cross partition query. 
            // We set the max concurrency to 4, which controls the max number of partitions that our client will query in parallel.
            var query = items.CreateItemQuery<TodoItem>(querySpec, maxConcurrency: 4);

            List<TodoItem> results = new List<TodoItem>();
            while (query.HasMoreResults)
            {
                var set = await query.FetchNextSetAsync();
                results.AddRange(set);
            }

            return results;
        }

        public static async Task<TodoItem> CreateItemAsync(TodoItem item)
        {
            if(item.Id == null)
            {
                item.Id = Guid.NewGuid().ToString();
            }
            return await items.CreateItemAsync<TodoItem>(item.Category, item);
        }

        public static async Task<TodoItem> UpdateItemAsync(TodoItem item)
        {
            return await items.ReplaceItemAsync<TodoItem>(item.Category, item.Id, item);
        }

        public static async Task DeleteItemAsync(string id, string category)
        {
            await items.DeleteItemAsync<TodoItem>(category, id);
        }

        public static async Task Initialize()
        {
            CosmosConfiguration config = new CosmosConfiguration(Endpoint, PrimaryKey);
            client = new CosmosClient(config);

            CosmosDatabase database = await client.Databases.CreateDatabaseIfNotExistsAsync(DatabaseId);
            CosmosContainer container = await database.Containers.CreateContainerIfNotExistsAsync(ContainerId, "/category");
            items = container.Items;
        }
    }
}
