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
using Microsoft.Azure.Documents.ChangeFeedProcessor;

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
            //await Program.DemoMongoAPI();
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
            var queryable2 = client.CreateDocumentQuery<PersonTableEntry>(collection.SelfLink, feedOptions).Where( doc => (doc.FirstName.v=="Bilbo") );

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
            Person aPerson = null;

            do
            {

                var queryable = client.CreateDocumentQuery<Person>(coll.SelfLink).Where(p => p.id == "001");
                var query = queryable.AsDocumentQuery();
                while (query.HasMoreResults)
                {
                    var personCol = await query.ExecuteNextAsync<Person>();
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
        /// <summary>
        /// This demo shows how you can use the ChangeFeed Functionality in CosmosDB
        /// Requires Nuget package: Microsoft.Azure.DocumentDB.ChangeFeedProcessor
        /// </summary>
        /// <returns></returns>
        static async Task DemoChangeFeed()
        {
            try
            {
                // Create a new Demo Collection "personcol" in "demodb"-Database
                Console.Write("Creating collection 'personcol' in 'demodb' (Demo_Build_Mongo Account),...");
                DocumentClient client = await CosDB.ConnectToCosmosDB(Program.Account_DemoBuild_Mongo, Program.Account_DemoBuild_Mongo_Key);
                Database db = await CosDB.CreateOrGetDatabase(client, "demodb");
                DocumentCollection personCol = await CosDB.CreateOrGetCollection(client, db, "personcol", 400, "/city", null, false,true);
                Console.WriteLine("Done");

                // Add several documents with corresponding partition keys
                Console.Write("Adding serveral 'person'-documents to personcol,...");
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name="Isabelle", city="Vienna",label="person"  });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Michaela", city="Vienna",label="person"  });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Michelle", city= "Paris", label="person"  });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Chantelle", city= "Paris", label="person"  });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name="Marion", city="Prag",label="person"  });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name= "Maxime", city="Prag",label="person"  });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name="Nina", city="Munich",label= "person" });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name="Maria", city = "Munich",label= "person" });
                Console.WriteLine("Done");

                #region Manually read Change Feed with Partitionkey ranges
                Console.WriteLine("DEMO - Reading Change Feed manually from beginnning");
                // Retrieve Partitionkey ranges to process large collection with multiple consumers.
                // You can get a list of all internal parition ranges. We have 4 Partitionkeys in our sample
                // but will only get one PartitionKeyRange for this (and it uses its internally Hashes!)
                string pkRangesResponseContinuation = null;
                List<PartitionKeyRange> partitionKeyRanges = new List<PartitionKeyRange>();
                do
                {
                    FeedResponse<PartitionKeyRange> pkRangesResponse = await client.ReadPartitionKeyRangeFeedAsync(
                        personCol.SelfLink,
                        new FeedOptions { RequestContinuation = pkRangesResponseContinuation });
                    partitionKeyRanges.AddRange(pkRangesResponse);
                    pkRangesResponseContinuation = pkRangesResponse.ResponseContinuation;
                }
                while (pkRangesResponseContinuation != null);
                // The internal partitionkey ranges are stored in internal properties: MinInclusive and MaxInclusive
                string x_ms_documentdb_partitionkeyrangeid = partitionKeyRanges.First().Id;

                Dictionary<string, string> checkpoints = new Dictionary<string, string>();
                // Process each partitionkey range
                foreach (PartitionKeyRange pkRange in partitionKeyRanges)
                {
                    string continuation = null;
                    checkpoints.TryGetValue(pkRange.Id, out continuation);
                    // Get the current change feed and start from the time the collection had been created.
                    // Fetch 1 document at a time
                    IDocumentQuery<Document> query = client.CreateDocumentChangeFeedQuery(
                        personCol.SelfLink,
                        new ChangeFeedOptions
                        {
                            PartitionKeyRangeId = pkRange.Id,
                            StartFromBeginning = true,
                            RequestContinuation = continuation,
                            MaxItemCount = 1
                        });

                    while (query.HasMoreResults)
                    {
                        FeedResponse<Person> readChangesResponse = query.ExecuteNextAsync<Person>().Result;
                        foreach (Person changedDocument in readChangesResponse)
                        {   // Will return one document at a time, see above in ChangeFeedOptions
                            Console.WriteLine(changedDocument.id);
                        }
                        checkpoints[pkRange.Id] = readChangesResponse.ResponseContinuation;
                    }
                }
                #endregion

                // Use the ChangeFeed Processor which manages to read changes accross partitions.
                // This requires another CosmosDB collection so that ChangeFeedProcessor can store it's lease information!
                // https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed#change-feed-processor
                #region Change Feed Processor
                // Create LeaseCollection 
                DocumentCollection leasecol = await CosDB.CreateOrGetCollection(client, db, "leasecol", 400, null, null, false, true);
                
                // Customizable change feed option and host options 
                ChangeFeedOptions feedOptions = new ChangeFeedOptions();
                // ie customize StartFromBeginning so change feed reads from beginning
                // can customize MaxItemCount, PartitonKeyRangeId, RequestContinuation, SessionToken and StartFromBeginning
                feedOptions.StartFromBeginning = true;
                ChangeFeedHostOptions feedHostOptions = new ChangeFeedHostOptions();
                // ie. customizing lease renewal interval to 15 seconds
                // can customize LeaseRenewInterval, LeaseAcquireInterval, LeaseExpirationInterval, FeedPollDelay 
                feedHostOptions.LeaseRenewInterval = TimeSpan.FromSeconds(15);

                string hostName = "UniqueHostName01";
                DocumentCollectionInfo documentCollectionLocation = new DocumentCollectionInfo()
                {
                     CollectionName=personCol.Id,
                     ConnectionPolicy=ConnectionPolicy.Default,
                      DatabaseName=db.Id,
                      MasterKey=Program.Account_DemoBuild_Mongo_Key,
                      Uri=new Uri(Program.Account_DemoBuild_Mongo)
                };
                DocumentCollectionInfo leaseCollectionLocation = new DocumentCollectionInfo()
                {
                    CollectionName = "leasecol",
                    ConnectionPolicy = ConnectionPolicy.Default,
                    DatabaseName = db.Id,
                    MasterKey = Program.Account_DemoBuild_Mongo_Key,
                    Uri = new Uri(Program.Account_DemoBuild_Mongo)
                };

                DocumentFeedObserverFactory docObserverFactory = new DocumentFeedObserverFactory();
                ChangeFeedEventHost host = new ChangeFeedEventHost(hostName, 
                    documentCollectionLocation, 
                    leaseCollectionLocation, 
                    feedOptions, feedHostOptions);

                await host.RegisterObserverFactoryAsync(docObserverFactory);

                Console.WriteLine("Press enter to add more documents...");
                Console.ReadLine();
                
                Random randGen = new Random((int)DateTime.Now.Ticks);
                List<string> cities = new List<string>() { "Vienna", "Paris", "Frankfurt", "Prag", "Seattle" };
                for (int c=0;c<10000;c++) // with this long running sample, the Lease is probably lost
                {
                    string city = cities[randGen.Next() % cities.Count];
                    await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = Guid.NewGuid().ToString("D"), city = city, label = "person" });
                    await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = Guid.NewGuid().ToString("D"), city = city, label = "person" });
                    await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = Guid.NewGuid().ToString("D"), city = city, label = "person" });
                }

                Console.WriteLine("Observer is still running... Modify or add documents with DocumentStudio. Press enter to stop.");
                Console.ReadLine();

                await host.UnregisterObserversAsync();
                #endregion
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
