using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Graphs;
using Microsoft.Azure.Graphs.Elements;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI.GraphDemo
{
    public class Demo
    {
        #region Graph-API

        public static async Task DemoGraphAPI()
        {
            await DemoGraph01();
        }

        public static async Task DemoGraph01()
        {
            try
            {
                DocumentClient client = await CosDB.ConnectToCosmosDB(Config.Account_DemoBuild_Hobbit, Config.Account_DemoBuild_Hobbit_Key);
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
    }
}
