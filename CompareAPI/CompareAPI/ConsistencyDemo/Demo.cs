using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI.ConsistencyDemo
{
    public class Demo
    {
        #region Consistency Demos
        public static async Task DemoConsistency()
        {
            await DemoSessionToken();
            await DemoOptimisticWrite();
        }

        /// <summary>
        /// Works on all consistency levels!
        /// </summary>
        /// <returns></returns>
        static async Task DemoOptimisticWrite()
        {
            try
            {
                DocumentClient client = await CosDB.ConnectToCosmosDB(Config.Account_DemoBuild_Docs, Config.Account_DemoBuild_Docs_Key);
                Database db = await CosDB.CreateOrGetDatabase(client, "demodb");
                DocumentCollection collection = await CosDB.CreateOrGetCollection(client, db, "democol", 400, null, null, false);
                var createResponse = await client.CreateDocumentAsync(collection.SelfLink, new { id = "001", name = "Book" });
                var replaceResponse = await client.ReplaceDocumentAsync(
                    createResponse.Resource.SelfLink,
                    new { id = "001", name = "Book", Title = "The Hobbit" },
                    new RequestOptions
                    {
                        AccessCondition = new Microsoft.Azure.Documents.Client.AccessCondition
                        {
                            Condition = createResponse.Resource.ETag,
                            Type = AccessConditionType.IfMatch
                        }
                    });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DemoOptimisticWrite failed with {ex.Message}.");
            }
        }

        static async Task DemoSessionToken()
        {
            try
            {
                DocumentClient client = await CosDB.ConnectToCosmosDB(Config.Account_DemoBuild_Docs, Config.Account_DemoBuild_Docs_Key);
                Database db = await CosDB.CreateOrGetDatabase(client, "demodb");
                DocumentCollection collection = await CosDB.CreateOrGetCollection(client, db, "democol", 400, null, null, false);

                //var queryable = client.CreateDocumentQuery(collection.SelfLink, "SELECT * FROM books WHERE books.id='001' ", new FeedOptions() { });
                var queryable = client.CreateDocumentQuery(collection.SelfLink, "SELECT * FROM books WHERE books.id='001' ", new FeedOptions() { SessionToken = "0:16" });
                var query = queryable.AsDocumentQuery();
                var result = await query.ExecuteNextAsync();

                var sessionToken = result.SessionToken;

                var createResponse = await client.CreateDocumentAsync(collection.SelfLink, new { id = "001", name = "Book" });
                var replaceResponse = await client.ReplaceDocumentAsync(
                    createResponse.Resource.SelfLink,
                    new { id = "001", name = "Book", Title = "The Hobbit" },
                    new RequestOptions
                    {
                        AccessCondition = new Microsoft.Azure.Documents.Client.AccessCondition
                        {
                            Condition = createResponse.Resource.ETag,
                            Type = AccessConditionType.IfMatch
                        }
                    });
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DemoSessionToken failed with {ex.Message}.");
            }
        }
        #endregion
    }
}
