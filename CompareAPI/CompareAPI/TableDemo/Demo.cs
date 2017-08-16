using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI.TableDemo
{
    public class Demo
    {
        #region Table-API
        public static async Task DemoTableAPI()
        {
            await DemoTableWorkWithTableAPI();
            await DemoTableWorkWithDocDBAPI();
        }
        public static async Task DemoTableWorkWithTableAPI()
        {
            // Create Table Entities
            PersonEntity gandalf = new PersonEntity() { FirstName = "Gandalf", LastName = "TheGray", Age = 200, Energy = 100.0d, Race = Race.Human };
            gandalf.CreateRowKey();
            PersonEntity bilbo = new PersonEntity() { FirstName = "Bilbo", LastName = "Beutlin", Age = 50, Energy = 100.0d, Race = Race.Hobbit };
            bilbo.CreateRowKey();
            PersonEntity thorin = new PersonEntity() { FirstName = "Thorin", LastName = "Eichenschild", Age = 250, Energy = 100.0d, Race = Race.Dwarf };
            thorin.CreateRowKey();

            // Create Cloud Table
            CloudStorageAccount storage = CloudStorageAccount.Parse(Config.Account_DemoBuild_Table_ConnectionString);
            CloudTableClient tableClient = storage.CreateCloudTableClient();
            CloudTable persons = tableClient.GetTableReference("person");
            await persons.CreateIfNotExistsAsync();

            // Add Table Entities
            TableOperation insertOP = TableOperation.InsertOrReplace(gandalf);
            await persons.ExecuteAsync(insertOP);
            insertOP = TableOperation.InsertOrReplace(bilbo);
            await persons.ExecuteAsync(insertOP);
            insertOP = TableOperation.InsertOrReplace(thorin);
            await persons.ExecuteAsync(insertOP);

            // Query Table Entities with LINQ
            TableQuery<PersonEntity> query = new TableQuery<PersonEntity>()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "Hobbit"),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("LastName", QueryComparisons.Equal, "Beutlin")
                ));

            var result = await persons.ExecuteQuerySegmentedAsync<PersonEntity>(query, null);
        }
        public static async Task DemoTableWorkWithDocDBAPI()
        {
            DocumentClient client = await CosDB.ConnectToCosmosDB(Config.Account_DemoBuild_Table, Config.Account_DemoBuild_Table_Key);
            Database db = await CosDB.CreateOrGetDatabase(client, "TablesDB");
            DocumentCollection collection = await CosDB.CreateOrGetCollection(client, db, "person", 400, "/'$pk'", null, false);

            FeedOptions feedOptions = new FeedOptions() { PartitionKey = new PartitionKey("Hobbit") };

            var queryable = client.CreateDocumentQuery(collection.SelfLink, "SELECT * from p where p['FirstName']['$v'] = 'Bilbo'", feedOptions);
            // queryable = client.CreateDocumentQuery<PersonEntity>(collection.SelfLink, feedOptions).Where( doc => (doc.FirstName=="Bilbo") );
            var queryable2 = client.CreateDocumentQuery<PersonTableEntry>(collection.SelfLink, feedOptions).Where(doc => (doc.FirstName.v == "Bilbo"));

            var query2 = queryable2.AsDocumentQuery<PersonTableEntry>();
            while (query2.HasMoreResults)
            {
                var person = await query2.ExecuteNextAsync<PersonTableEntry>();
            }

            var query = queryable.AsDocumentQuery();
            while (query.HasMoreResults)
            {
                var person = await query.ExecuteNextAsync();
            }
        }
        #endregion

    }
}
