using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI.GeoDistributionDemo
{
    public class Demo
    {
        #region GeoConnect

        public static async Task DemoGeoConnect()
        {
            // Make sure we prefer Japan East over North Europe
            var prefLocations = new List<string> { LocationNames.JapanEast /*, LocationNames.NorthEurope */ };
            DocumentClient client = await CosDB.ConnectToCosmosDB(Config.Account_GlobalBuildDemo, Config.Account_GlobalBuildDemo_Key, prefLocations);
            Database db = await CosDB.CreateOrGetDatabase(client, "demodb");
            DocumentCollection coll = await CosDB.CreateOrGetCollection(client, db, "democol", 400, null, null, false);
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
            } while (aPerson == null);
        }

        #endregion
    }
}
