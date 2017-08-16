using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.ChangeFeedProcessor;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace CompareAPI.ChangeFeedDemo
{
    public class Demo
    {
        /// <summary>
        /// This demo shows how you can use the ChangeFeed Functionality in CosmosDB
        /// Requires Nuget package: Microsoft.Azure.DocumentDB.ChangeFeedProcessor
        /// This demo requires:
        ///     CosmosDB Account set up and configuration of
        ///         Account_DemoBuild_Mongo
        ///         Account_DemoBuild_Mongo_Key
        /// It will create a demodb-database 
        ///     A partitioned personcol-Collection 
        ///     A non paritioned leasecol-Collection
        /// </summary>
        /// <returns></returns>
        public static async Task DemoChangeFeed()
        {
            try
            {
                // Create a new Demo Collection "personcol" in "demodb"-Database
                Console.Write("Creating collection 'personcol' in 'demodb' (Demo_Build_Mongo Account),...");
                DocumentClient client = await CosDB.ConnectToCosmosDB(Config.Account_DemoBuild_Mongo, Config.Account_DemoBuild_Mongo_Key);
                Database db = await CosDB.CreateOrGetDatabase(client, "demodb");
                DocumentCollection personCol = await CosDB.CreateOrGetCollection(client, db, "personcol", 400, "/city", null, false, true);
                Console.WriteLine("Done");

                // Add several documents with corresponding partition keys
                Console.Write("Adding serveral 'person'-documents to personcol,...");
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Isabelle", city = "Vienna", label = "person" });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Michaela", city = "Vienna", label = "person" });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Michelle", city = "Paris", label = "person" });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Chantelle", city = "Paris", label = "person" });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Marion", city = "Prag", label = "person" });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Maxime", city = "Prag", label = "person" });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Nina", city = "Munich", label = "person" });
                await client.CreateDocumentAsync(personCol.SelfLink, new Person() { name = "Maria", city = "Munich", label = "person" });
                Console.WriteLine("Done");

                // Demonstrate how to read the Change Feed manually
                // Uncommented for easier repo
                // await ProcessChangeFeedManualy(client, personCol);

                // Demonstrate the use of the ChangeFeed Processor
                await ProcessChangeFeedWithChangeFeedProcessor(client, db, personCol);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Change Feed Demo failed with {ex.Message}.");
                throw;
            }
        }

        private static async Task ProcessChangeFeedWithChangeFeedProcessor(DocumentClient client, Database db, DocumentCollection personCol)
        {
            // Use the ChangeFeed Processor which manages to read changes accross partitions.
            // This requires another CosmosDB collection so that ChangeFeedProcessor can store it's lease information!
            // https://docs.microsoft.com/en-us/azure/cosmos-db/change-feed#change-feed-processor
            #region Change Feed Processor
            // Create LeaseCollection 
            DocumentCollection leasecol = await CosDB.CreateOrGetCollection(client, db, "leasecol", 400, null, null, false, true);

            // Customizable change feed option and host options 
            ChangeFeedOptions feedOptions = new ChangeFeedOptions()
            {
                // ie customize StartFromBeginning so change feed reads from beginning
                // can customize MaxItemCount, PartitonKeyRangeId, RequestContinuation, SessionToken and StartFromBeginning
                StartFromBeginning = true
            };
            ChangeFeedHostOptions feedHostOptions = new ChangeFeedHostOptions()
            {
                // ie. customizing lease renewal interval to 15 seconds
                // can customize LeaseRenewInterval, LeaseAcquireInterval, LeaseExpirationInterval, FeedPollDelay 
                LeaseRenewInterval = TimeSpan.FromSeconds(15)
            };
            string hostName = "UniqueHostName01";
            DocumentCollectionInfo documentCollectionLocation = new DocumentCollectionInfo()
            {
                CollectionName = personCol.Id,
                ConnectionPolicy = ConnectionPolicy.Default,
                DatabaseName = db.Id,
                MasterKey = Config.Account_DemoBuild_Mongo_Key,
                Uri = new Uri(Config.Account_DemoBuild_Mongo)
            };
            DocumentCollectionInfo leaseCollectionLocation = new DocumentCollectionInfo()
            {
                CollectionName = "leasecol",
                ConnectionPolicy = ConnectionPolicy.Default,
                DatabaseName = db.Id,
                MasterKey = Config.Account_DemoBuild_Mongo_Key,
                Uri = new Uri(Config.Account_DemoBuild_Mongo)
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
            for (int c = 0; c < 10000; c++) // with this long running sample, the Lease is probably lost
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

        private static async Task ProcessChangeFeedManualy(DocumentClient client, DocumentCollection personCol)
        {
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
                checkpoints.TryGetValue(pkRange.Id, out string continuation);
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
        }

    }
}
