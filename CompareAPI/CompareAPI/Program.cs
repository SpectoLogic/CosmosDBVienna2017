using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Graphs;
using Microsoft.Azure.Graphs.Elements;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;
using System.Linq;
using MongoDB.Driver;
using MongoDB.Driver.Linq;

namespace CompareAPI
{
    class Program
    {
        #region Configuration
        static string Account_DemoBuild_Hobbit;
        static string Account_DemoBuild_Hobbit_Graph;
        static string Account_DemoBuild_Hobbit_Key;

        static string Account_DemoBuild_Mongo;
        static string Account_DemoBuild_Mongo_Key;
        static string Account_DemoBuild_Mongo_ConnectionString;

        static string Account_DemoBuild_Docs;
        static string Account_DemoBuild_Docs_Key;

        static string Account_DemoBuild_Table;
        static string Account_DemoBuild_Table_Key;
        static string Account_DemoBuild_Table_ConnectionString;

        static string Account_GlobalBuildDemo;
        static string Account_GlobalBuildDemo_Key;
        #endregion

        #region Main
        static void Main(string[] args)
        {
            #region Read Configuration
            Account_DemoBuild_Hobbit = ConfigurationManager.AppSettings["Account_DemoBuild_Hobbit"];
            Account_DemoBuild_Hobbit_Graph = ConfigurationManager.AppSettings["Account_DemoBuild_Hobbit_Graph"];
            Account_DemoBuild_Hobbit_Key = ConfigurationManager.AppSettings["Account_DemoBuild_Hobbit_Key"];

            Account_DemoBuild_Mongo = ConfigurationManager.AppSettings["Account_DemoBuild_Mongo"];
            Account_DemoBuild_Mongo_Key = ConfigurationManager.AppSettings["Account_DemoBuild_Mongo_Key"];
            Account_DemoBuild_Mongo_ConnectionString = ConfigurationManager.AppSettings["Account_DemoBuild_Mongo_ConnectionString"];

            Account_DemoBuild_Docs = ConfigurationManager.AppSettings["Account_DemoBuild_Docs"];
            Account_DemoBuild_Docs_Key = ConfigurationManager.AppSettings["Account_DemoBuild_Docs_Key"];

            Account_DemoBuild_Table = ConfigurationManager.AppSettings["Account_DemoBuild_Table"];
            Account_DemoBuild_Table_Key = ConfigurationManager.AppSettings["Account_DemoBuild_Table_Key"];
            Account_DemoBuild_Table_ConnectionString = ConfigurationManager.AppSettings["Account_DemoBuild_Table_ConnectionString"];

            Account_GlobalBuildDemo = ConfigurationManager.AppSettings["Account_GlobalBuildDemo"];
            Account_GlobalBuildDemo_Key = ConfigurationManager.AppSettings["Account_GlobalBuildDemo_Key"];
            #endregion

            Task.Run(async () =>
            {
                try
                {
                    await Program.DemoMain();
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed with {ex.Message}.");
                }
            }).Wait();

            Console.WriteLine("Done");
            Console.ReadLine();
        }

        static async Task DemoMain()
        {
            await Program.DemoChangeFeed();
            await Program.DemoMongoAPI();
            //await Program.DemoTableAPI();
            //await Program.DemoConsistency();
            //await Program.DemoGraphAPI();
            //await Program.DemoGeoConnect();
        }
        #endregion

        #region Table-API
        static async Task DemoTableAPI()
        {
            await DemoTableWorkWithTableAPI();
            await DemoTableWorkWithDocDBAPI();
        }
        static async Task DemoTableWorkWithTableAPI()
        {
            // Create Table Entities
            PersonEntity gandalf = new PersonEntity() { FirstName = "Gandalf", LastName = "TheGray", Age = 200, Energy = 100.0d, Race = Race.Human };
            gandalf.CreateRowKey();
            PersonEntity bilbo = new PersonEntity() { FirstName = "Bilbo", LastName = "Beutlin", Age = 50, Energy = 100.0d, Race = Race.Hobbit };
            bilbo.CreateRowKey();
            PersonEntity thorin = new PersonEntity() { FirstName = "Thorin", LastName = "Eichenschild", Age = 250, Energy = 100.0d, Race = Race.Dwarf };
            thorin.CreateRowKey();

            // Create Cloud Table
            CloudStorageAccount storage = CloudStorageAccount.Parse(Account_DemoBuild_Table_ConnectionString);
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
        static async Task DemoTableWorkWithDocDBAPI()
        {
            DocumentClient client = await CosDB.ConnectToCosmosDB(Account_DemoBuild_Table, Account_DemoBuild_Table_Key);
            Database db = await CosDB.CreateOrGetDatabase(client, "TablesDB");
            DocumentCollection collection = await CosDB.CreateOrGetCollection(client, db, "person", 400, "/'$pk'", null, false);

            FeedOptions feedOptions = new FeedOptions() { PartitionKey = new PartitionKey("Hobbit") };

            var queryable = client.CreateDocumentQuery(collection.SelfLink, "SELECT * from p where p['FirstName']['$v'] = 'Bilbo'", feedOptions);
            // queryable = client.CreateDocumentQuery<PersonEntity>(collection.SelfLink, feedOptions).Where( doc => (doc.FirstName=="Bilbo") );
            var queryable2 = client.CreateDocumentQuery<PersonT>(collection.SelfLink, feedOptions).Where( doc => (doc.FirstName.v=="Bilbo") );

            var query2 = queryable2.AsDocumentQuery<PersonT>();
            while (query2.HasMoreResults)
            {
                var person = await query2.ExecuteNextAsync<PersonT>();
            }

            var query = queryable.AsDocumentQuery();
            while (query.HasMoreResults)
            {
                var person = await query.ExecuteNextAsync();
            }
        }
        #endregion

        #region Consistency Demos
        static async Task DemoConsistency()
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
                DocumentClient client = await CosDB.ConnectToCosmosDB(Account_DemoBuild_Docs, Account_DemoBuild_Docs_Key);
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
                DocumentClient client = await CosDB.ConnectToCosmosDB(Account_DemoBuild_Docs, Account_DemoBuild_Docs_Key);
                Database db = await CosDB.CreateOrGetDatabase(client, "demodb");
                DocumentCollection collection = await CosDB.CreateOrGetCollection(client, db, "democol", 400, null, null, false);
                
                //var queryable = client.CreateDocumentQuery(collection.SelfLink, "SELECT * FROM books WHERE books.id='001' ", new FeedOptions() { });
                var queryable = client.CreateDocumentQuery(collection.SelfLink, "SELECT * FROM books WHERE books.id='001' ", new FeedOptions() { SessionToken= "0:16" });
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

        #region Graph-API

        static async Task DemoGraphAPI()
        {
            await DemoGraph01();
        }

        static async Task DemoGraph01()
        {
            try
            {
                DocumentClient client = await CosDB.ConnectToCosmosDB(Account_DemoBuild_Hobbit, Account_DemoBuild_Hobbit_Key);
                Database db = await CosDB.CreateOrGetDatabase(client, "demodb");
                DocumentCollection collection = await CosDB.CreateOrGetCollection(client, db, "thehobbit", 400, null, null, false);

                var results = new List<dynamic>();
                var gremlinQuery = client.CreateGremlinQuery<dynamic>(collection, "g.V().hasLabel('person')");

                while (gremlinQuery.HasMoreResults)
                {
                    foreach (var result in await gremlinQuery.ExecuteNextAsync<dynamic>())
                    {
                        results.Add(result);
                    }
                }

                var typedGremlinQuery = client.CreateGremlinQuery<Vertex>(collection, "g.V().hasLabel('person')");
                while (typedGremlinQuery.HasMoreResults)
                {
                    foreach (var result in await typedGremlinQuery.ExecuteNextAsync<Vertex>())
                    {
                        Console.WriteLine(result.Label);
                        var props = result.GetVertexProperties();
                        var name = result.GetVertexProperties("name").First().Value;
                    }
                }

                // ================================================================================
                // The following is no longer supported with 0.2.4 update
                // The classes have been updated with INTERNAL USE only
                // ================================================================================
                /*
                GraphConnection graphConnection = await GraphConnection.Create(Account_DemoBuild_Hobbit, Account_DemoBuild_Hobbit_Key, "demodb", "thehobbit");
                Microsoft.Azure.Graphs.GraphCommand cmd = new GraphCommand(graphConnection);
                GraphTraversal personTrav = cmd.g().V().HasLabel("person");
                foreach(var p in personTrav)
                {
                    Console.WriteLine(p);
                }

                Microsoft.Azure.Graphs.GraphCommand cmd2 = new GraphCommand(graphConnection, "g.V().hasLabel('person').union(V().hasLabel('place'))");
                var res2 = cmd2.Execute();
                int cnt = 0;
                foreach (var xx in res2)
                {
                    Console.WriteLine(xx);
                    cnt++;
                }
                // 105 =>  g.V().hasLabel('person').union(V().hasLabel('place'))  .count()  .limit(1)
                */
            }
            catch (Exception ex)
            {
                Console.WriteLine($"GraphDemo 01 failed with {ex.Message}.");
            }
        }

        #endregion

        #region GeoConnect

        static async Task DemoGeoConnect()
        {
            // Make sure we prefer Japan East over North Europe
            var prefLocations = new List<string> { LocationNames.JapanEast /*, LocationNames.NorthEurope */ };
            DocumentClient client = await CosDB.ConnectToCosmosDB(Account_GlobalBuildDemo, Account_GlobalBuildDemo_Key,prefLocations);
            Database db = await CosDB.CreateOrGetDatabase(client, "demodb");
            DocumentCollection coll = await CosDB.CreateOrGetCollection(client,db,"democol",400,null,null,false);
            person aPerson = null;

            do
            {

                var queryable = client.CreateDocumentQuery<person>(coll.SelfLink).Where(p => p.id == "001");
                var query = queryable.AsDocumentQuery();
                while (query.HasMoreResults)
                {
                    var personCol = await query.ExecuteNextAsync<person>();
                    if (personCol.Count > 0)
                    {
                        aPerson = personCol.FirstOrDefault();
                    }
                }
                await Task.Delay(1000);
            } while (aPerson==null);
        }

        #endregion

        #region MongoDB
        static async Task DemoMongoAPI()
        {
            try
            {
                MongoClient client = new MongoClient(Program.Account_DemoBuild_Mongo_ConnectionString);
                var db = client.GetDatabase("demodb");
                try
                {
                    await db.DropCollectionAsync("democol"); 
                }catch (Exception)
                {
                }   
                var col = db.GetCollection<MongoItem>("democol");
                MongoItem itemA = new MongoItem() {
                    id = "84476d91-8fe4-4d8c-8525-feefb3e12912",
                    Container = "demo",
                    UploadUser = new MUser() { FirstName = "Hansi", LastName = "Huber" },
                    UserFlag = new string[] { "user1" },
                    TagList = new string[] {"car","crash"}
                };
                MongoItem itemB = new MongoItem()
                {
                    id = "2ba8a3a2-9937-408b-b5d3-0acef8ce0fb7",
                    Container = "marastore",
                    UploadUser = new MUser() { FirstName = "Mara", LastName = "Jade" },
                    UserFlag = new string[] { "user2" },
                    TagList = new string[] { "car", "luxus" }
                };
                await col.InsertOneAsync(itemA);
                await col.InsertOneAsync(itemB);

                /// Try to load documents that have their "Container" property
                /// start with "mara".
                var result = from items in col.AsQueryable<MongoItem>()
                         where items.Container.StartsWith("mara")
                         select items;
                foreach (var item in result)
                {
                    Console.WriteLine(item.Container);
                }
                
                /// Try to load documents that contain a certain element in their Tag List
                /// This will always return 0 results.
                result = from items in col.AsQueryable<MongoItem>()
                         where items.TagList.Contains<string>("car")
                         select items;
                foreach (var item in result)
                {
                    Console.WriteLine(item.id);
                }

                /// Again try to load the element by using Select Many
                /// Fails with Exception: "$project or $group does not support {document}."
                /// It seems not the full MongoDB API is supported. 
                /// TODO: Find more information about this.
                var tagResults = from a in col.AsQueryable<MongoItem>()
                           from b in a.TagList where b.StartsWith("car")
                           select b;
                foreach (var tag in tagResults)
                {
                    Console.WriteLine(tag);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"MongoDB Demo failed with {ex.Message}.");
                throw;
            }
        }
        #endregion

        #region Change Feed Demo
        static async Task DemoChangeFeed()
        {
            try
            {

            }
            catch (Exception ex)
            {
                Console.WriteLine($"Change Feed Demo failed with {ex.Message}.");
                throw;
            }
        }

        #endregion
    }
}
