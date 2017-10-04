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
    class MongoDemo
    {
        /// <summary>
        /// Change Connectionstring here to verify different behaviour
        /// </summary>
        public static MongoClient Client
        {
            get
            {
#if COSMOSDB
                return new MongoClient(Config.Account_DemoBuild_Mongo_ConnectionString);
#endif
#if MONGODB
                return new MongoClient(Config.Account_Bitnami_Mongo_ConnectionString);
#endif
            }
        }

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
                MongoItem demoDoc = new MongoItem("Hansi", "Huber", "Test", "ModeT", "",
                                                1.1, 2.2,
                                                new string[] { "male", "person" },
                                                new string[] { "hansi" });
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
                MongoClient client = MongoDemo.Client;
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

        public static async Task DemoMongoAPIDataTypes()
        {
            MongoClient client = MongoDemo.Client;
            var db = client.GetDatabase("demodb");

            try { await db.DropCollectionAsync("democol"); } catch (Exception) { }
            var col = db.GetCollection<MongoItem2>("democol");

            MongoItem2 item = new MongoItem2();
            col.InsertOne(item);

            // This query only works with a original MongoDB (f.e. Bitnami Instance) but not with CosmosDB
            try
            {
                var filter = "{ LValue: {$type: 18} }"; /// { "LValue":{$type: 18} }               
                await col.Find(filter)
                    .ForEachAsync(document => Console.WriteLine(document.LValue));

                filter = "{ DValue:{$type: 1} }"; /// { "DValue":{$type: 1} }               
                await col.Find(filter)
                    .ForEachAsync(document => Console.WriteLine(document.DValue));
            }
            catch (MongoCommandException ex)
            {
                Console.WriteLine($"Type query failed {ex.Result}.");
            }

            //var result = from items in col.AsQueryable<MongoItem2>()
            //             where items.DValue is Double
            //             select items;
            //foreach (var i in result)
            //{
            //    Console.WriteLine(i.DValue );
            //}


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
                MongoClient client = MongoDemo.Client;
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
                MongoClient client = MongoDemo.Client;
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
                /// Bitnami:    Returns 1 result.
                /// CosmosDB:   Returns 1 result
                /// {aggregate([{ "$match" : { "City" : /^mara/s } }])}
                var result = from items in col.AsQueryable<MongoItem>()
                             where items.City.StartsWith("mara")
                             select items;
                foreach (var item in result)
                {
                    Console.WriteLine(item.City);
                }

                /// Bitnami:    Returns 1 result.
                /// CosmosDB:   Returns 1 result
                // {aggregate([{ "$match" : { "DemoUser.FirstName" : /^Hansi/s } }])}
                result = from items in col.AsQueryable<MongoItem>()
                             where items.DemoUser.FirstName.StartsWith("Hansi")
                             select items;
                foreach (var item in result)
                {
                    Console.WriteLine(item.City);
                }


                /// Try to load documents that contain a certain element in their Tag List
                /// This will always return 0 results with DocumentDB
                /// This will return the correct result with a Bitnami MongoDB Instance
                /// Bitnami ==>     2 Results
                /// CosmosDB ==>    0 Results
                /// It seems not the full MongoDB API is supported with CosmosDB or this is a bug! 
                /// {aggregate([{ "$match" : { "TagList" : "car" } }])}
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
                // This query:
                //  var tagResults =    from a in col.AsQueryable<MongoItem>()
                //                      from b in a.TagList
                //                      where b.StartsWith("car")
                //                      select b;
                // Cannot be created

                // This is my manual query which would retrieve the relevant Objects
                // Returns a result in  Bitnami
                // In CosmosDB if fails because $unwind is not supported. If you remove $unwind it still does not return any results.
                // db.democol.aggregate( [ { $unwind : "$TagList" }, { "$project" : { "TagList" : "$TagList", "_id" : "$_id" } },{$match : {"TagList":"car"} } ])
                // 
                // This query
                //  var tagResults =    from a in col.AsQueryable<MongoItem>()
                //                      from b in a.TagList
                //                      select b;
                // Results in: {aggregate([{ "$unwind" : "$TagList" }, { "$project" : { "TagList" : "$TagList", "_id" : 0 } }])}
                //
                // This query
                //      var tagResults = from a in col.AsQueryable()
                //                      from b in a.TagList
                //                      select new { id = a.id, tag = b };
                // Translates to: {aggregate([{ "$unwind" : "$TagList" }, { "$project" : { "id" : "$_id", "tag" : "$TagList", "_id" : 0 } }])}

                // https://stackoverflow.com/questions/36601909/mongodb-c-sharp-query-nested-document

                //var tagResults = col.AsQueryable<MongoItem>()
                //                .SelectMany(e => e.TagList, (e, b) => new { entityA = e, bitem = b })
                //                .Where(t => "car" == t.bitem)
                //                .Select(t => t.bitem);
                // "$project or $group does not support {document}."

                //var tagResults = col.AsQueryable<MongoItem>()
                //          .Select(e => new { e.id, e.TagList });
                // {aggregate([{ "$project" : { "id" : "$_id", "TagList" : "$TagList", "_id" : 0 } }])}

                var tagResults = col.AsQueryable<MongoItem>()
                                 .SelectMany(e => e.TagList, (e, b) => new { A = e, B = b });
                // Bitnami: Error => "$project or $group does not support {document}."
                foreach (var tag in tagResults)
                {
                    Console.WriteLine(tag);
                }

                // http://mongodb.github.io/mongo-csharp-driver/2.4/reference/driver/expressions/#filters
                // http://mongodb.github.io/mongo-csharp-driver/2.4/reference/driver/crud/linq/#match
                // https://www.codementor.io/pmbanugo/working-with-mongodb-in-net-2-retrieving-mrlbeanm5

                // db.democol.find({ "TagList" : "crash" })
                // Bitnami ==> Return 2 results
                using (IAsyncCursor<MongoItem> cursor = await col.FindAsync(p => p.TagList.Any(t => t == "car")))
                {
                    while (await cursor.MoveNextAsync())
                    {
                        IEnumerable<MongoItem> batch = cursor.Current;
                        foreach (MongoItem document in batch)
                        {
                            Console.WriteLine(document);
                        }
                    }
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
