using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI.MongoDBDemo
{
    class Demo
    {
        #region MongoDB
        /// <summary>
        /// This stores a document into CosmosDB. First the document is stored with the 
        /// DocumentDB-API and then with the MongoAPI. Explore both Location-Elements:
        ///  * "LocationMongoDB" and
        ///  * "LocationCosmosDB" 
        ///  and their representation in the database.
        /// </summary>
        /// <returns></returns>
        public static async Task DemoStorageOfLocation()
        {
            try
            {
                MongoItem demoDoc = new MongoItem("Hansi", "Huber", "Test", "ModeT", "", 1.1, 2.2, new string[] { "male", "person" }, new string[] { "hansi" });
                // =======================================================
                // Store in CosmosDB
                // =======================================================
                DocumentClient docDBclient = await CosDB.ConnectToCosmosDB(Config.Account_DemoBuild_Mongo, Config.Account_DemoBuild_Mongo_Key);
                Database docDBdb = await CosDB.CreateOrGetDatabase(docDBclient, "demodb");
                DocumentCollection collection = await CosDB.CreateOrGetCollection(docDBclient, docDBdb, "democolDocDB", 400, null, null, false);
                await docDBclient.CreateDocumentAsync(collection.SelfLink, demoDoc);
                // =======================================================
                // Store with MongoDB API
                // =======================================================
                MongoQueryTest queryTest = new MongoQueryTest();
                MongoClient client = new MongoClient(Config.Account_DemoBuild_Mongo_ConnectionString);
                //MongoClient client = new MongoClient(Config.Account_Bitnami_Mongo_ConnectionString);
                var db = client.GetDatabase("demodb");
                try
                {
                    await db.DropCollectionAsync("democol");
                }
                catch (Exception)
                {
                }
                var col = db.GetCollection<MongoItem>("democol");
                await col.InsertOneAsync(demoDoc);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"DemoMongoAPILocation Demo failed with {ex.Message}.");
                throw;
            }
        }
        /// <summary>
        /// Shows the behaviour of CosmosDB when access with Linq for MongoDB driver
        /// in contrary to a standard mongodb instantiated via Bitnami as virtual machine f.e.
        /// </summary>
        /// <returns></returns>
        public static async Task DemoMongoAPIQueries2()
        {
            try
            {
                //MongoClient client = new MongoClient(Config.Account_DemoBuild_Mongo_ConnectionString);
                MongoClient client = new MongoClient(Config.Account_Bitnami_Mongo_ConnectionString);
                var db = client.GetDatabase("demodb");
                try
                {
                    await db.DropCollectionAsync("democol");
                }
                catch (Exception)
                {
                }
                var col = db.GetCollection<MongoItem>("democol");
                MongoQueryTest queryTest = new MongoQueryTest();
                await queryTest.AddItemsToMongoDBCollection(col, queryTest.CreateTestData());
                queryTest.TestExpressionQueries(col, col.AsQueryable<MongoItem>());
            }
            catch (Exception ex)
            {
                Console.WriteLine($"QueryDemoMongoAPI Demo failed with {ex.Message}.");
                throw;
            }
        }

        /// <summary>
        /// Shows the behaviour of CosmosDB when access with Linq for MongoDB driver
        /// in contrary to a standard mongodb instantiated via Bitnami as virtual machine f.e.
        /// </summary>
        /// <returns></returns>
        public static async Task DemoMongoAPIQueries1()
        {
            try
            {
                MongoClient client = new MongoClient(Config.Account_DemoBuild_Mongo_ConnectionString);
                //MongoClient client = new MongoClient(Config.Account_Bitnami_Mongo_ConnectionString);
                var db = client.GetDatabase("demodb");
                try
                {
                    await db.DropCollectionAsync("democol");
                }
                catch (Exception)
                {
                }
                var col = db.GetCollection<MongoItem>("democol");

                MongoItem itemA = new MongoItem()
                {
                    id = "84476d91-8fe4-4d8c-8525-feefb3e12912",
                    City = "demo",
                    DemoUser = new MUser() { FirstName = "Hansi", LastName = "Huber" },
                    UserList = new string[] { "user1" },
                    TagList = new string[] { "car", "crash" }
                };
                MongoItem itemB = new MongoItem()
                {
                    id = "2ba8a3a2-9937-408b-b5d3-0acef8ce0fb7",
                    City = "marastore",
                    DemoUser = new MUser() { FirstName = "Mara", LastName = "Jade" },
                    UserList = new string[] { "user2" },
                    TagList = new string[] { "car", "luxus" }
                };
                await col.InsertOneAsync(itemA);
                await col.InsertOneAsync(itemB);

                /// Try to load documents that have their "Container" property
                /// start with "mara".
                var result = from items in col.AsQueryable<MongoItem>()
                             where items.City.StartsWith("mara")
                             select items;
                foreach (var item in result)
                {
                    Console.WriteLine(item.City);
                }

                /// Try to load documents that contain a certain element in their Tag List
                /// This will always return 0 results with DocumentDB
                /// This will return the correct result with a Bitnami MongoDB Instance
                /// It seems not the full MongoDB API is supported with CosmosDB or this is a bug! 
                result = from items in col.AsQueryable<MongoItem>()
                         where items.TagList.Contains<string>("car")
                         select items;
                foreach (var item in result)
                {
                    Console.WriteLine(item.id);
                }

                /// Again try to load the element by using Select Many
                /// Fails with Exception: "$project or $group does not support {document}."
                /// 
                /// Update: This also fails with a BitNami MongoDB Instance
                var tagResults = from a in col.AsQueryable<MongoItem>()
                                 from b in a.TagList
                                 where b.StartsWith("car")
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
    }
}
