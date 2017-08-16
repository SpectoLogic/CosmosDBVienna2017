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
        #region Entrypoint and Configuration
        static void Main(string[] args)
        {
            Program.ReadConfiguration();

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
        static void ReadConfiguration()
        {
            #region Read Configuration
            Config.Account_DemoBuild_Hobbit = ConfigurationManager.AppSettings["Account_DemoBuild_Hobbit"];
            Config.Account_DemoBuild_Hobbit_Graph = ConfigurationManager.AppSettings["Account_DemoBuild_Hobbit_Graph"];
            Config.Account_DemoBuild_Hobbit_Key = ConfigurationManager.AppSettings["Account_DemoBuild_Hobbit_Key"];

            Config.Account_DemoBuild_Mongo = ConfigurationManager.AppSettings["Account_DemoBuild_Mongo"];
            Config.Account_DemoBuild_Mongo_Key = ConfigurationManager.AppSettings["Account_DemoBuild_Mongo_Key"];
            Config.Account_DemoBuild_Mongo_ConnectionString = ConfigurationManager.AppSettings["Account_DemoBuild_Mongo_ConnectionString"];
            Config.Account_Bitnami_Mongo_ConnectionString = ConfigurationManager.AppSettings["Account_Bitnami_Mongo_ConnectionString"];

            Config.Account_DemoBuild_Docs = ConfigurationManager.AppSettings["Account_DemoBuild_Docs"];
            Config.Account_DemoBuild_Docs_Key = ConfigurationManager.AppSettings["Account_DemoBuild_Docs_Key"];

            Config.Account_DemoBuild_Table = ConfigurationManager.AppSettings["Account_DemoBuild_Table"];
            Config.Account_DemoBuild_Table_Key = ConfigurationManager.AppSettings["Account_DemoBuild_Table_Key"];
            Config.Account_DemoBuild_Table_ConnectionString = ConfigurationManager.AppSettings["Account_DemoBuild_Table_ConnectionString"];

            Config.Account_GlobalBuildDemo = ConfigurationManager.AppSettings["Account_GlobalBuildDemo"];
            Config.Account_GlobalBuildDemo_Key = ConfigurationManager.AppSettings["Account_GlobalBuildDemo_Key"];
            #endregion
        }
        #endregion

        /// <summary>
        /// Execution of Demo Code
        /// </summary>
        /// <returns></returns>
        static async Task DemoMain()
        {
            // Demonstrates the use of ChangeFeeds with Azure CosmosDB
            await ChangeFeedDemo.Demo.DemoChangeFeed();

            // Demonstrates the usage of MongoAPI of Azure CosmosDB and the 
            // difference between a native MongoDB instance (f.e. Bitnami-Instance)

            await MongoDBDemo.Demo.DemoMongoAPIQueries1();
            await MongoDBDemo.Demo.DemoMongoAPIQueries2();
            await MongoDBDemo.Demo.DemoStorageOfLocation();

            // Demonstrates the GraphAPI of Azure CosmosDB
            await GraphDemo.Demo.DemoGraphAPI();

            // Demonstrates the Premium Table API of Azure CosmosDB
            await TableDemo.Demo.DemoTableAPI();

            // Various experimentations with different consistency levels
            await ConsistencyDemo.Demo.DemoConsistency();

            // Demonstrates how to specifically connect to a specific region with Azure CosmosDB
            await GeoDistributionDemo.Demo.DemoGeoConnect();
        }
    }
}
