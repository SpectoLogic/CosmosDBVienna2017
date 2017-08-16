using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using CompareAPI.Linq;
using MongoDB.Driver;
using System.Linq.Expressions;
using MongoDB.Driver.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using System.Text.RegularExpressions;

namespace CompareAPI.MongoDBDemo
{
    public class MongoQueryTest
    {
        public void TestExpressionQueries(IMongoCollection<MongoItem> col, IMongoQueryable<MongoItem> query)
        {
            #region Various LINQ-Expressions to combine and test
            Expression<Func<MongoItem, bool>> cityExpressionVienna = (d => d.City == "Vienna");
            Expression<Func<MongoItem, bool>> cityExpressionGraz = (d => d.City == "Graz");
            Expression<Func<MongoItem, bool>> firstNameExpression = (d => d.DemoUser.FirstName == "Michaela");
            Expression<Func<MongoItem, bool>> firstNameExpression2 = (d => (d.DemoUser.FirstName == "Michaela" || d.City == "Graz"));
            Expression<Func<MongoItem, bool>> modeExpression = (d => (d.Mode == "ModeA" || d.Mode == "ModeB" || d.Mode == "ModeC"));
            Expression<Func<MongoItem, bool>> modeExpression2 = (d => (d.Mode != "ModeA" && d.Mode != "ModeB" && d.Mode != "ModeC"));

            DateTimeOffset sFrom = DateTimeOffset.ParseExact("2017:04:02 18:00:00", "yyyy:MM:dd HH:mm:ss", null).ToUniversalTime();
            DateTimeOffset sTo = DateTimeOffset.ParseExact("2017:04:03 15:59:00", "yyyy:MM:dd HH:mm:ss", null).ToUniversalTime();
            Expression<Func<MongoItem, bool>> isWithinDateRange = (d => (d.CheckPoint.CompareTo(sFrom) >= 0 && d.CheckPoint.CompareTo(sTo) <= 0));

            Expression<Func<MongoItem, bool>> containsTagFemale = (d => d.TagList.Contains<string>("female"));
            Expression<Func<MongoItem, bool>> containsUserID = (d => d.UserList.Contains<string>("martinau"));

            Expression<Func<MongoItem, DateTimeOffset>> orderByDate = t => t.CheckPoint;
            #endregion

            #region Build a LINQ-Query, turn it to JSON, create aggregate Pipeline with BSON from JSON
            PipelineDefinition<MongoItem, MongoItem> aggregatePipeline = null;
            // Build a more complex LINQ Query
            var query0 = query.Where(cityExpressionVienna).OrderBy("CheckPoint ASC").Skip(1).Take(2);
            // Use the overloaded toString()-method to retrieve the aggregate-query ==> "aggregate([{match-Expression},...])
            string mongoAggregateQueryString = query0.ToString();
            Regex aggregateParameters = new Regex(@"(aggregate\()(.*)(\)$)");
            mongoAggregateQueryString  = aggregateParameters.Match(mongoAggregateQueryString).Groups[2].Value;
            List<BsonDocument> docs = new List<BsonDocument>();
            var aggElements = BsonSerializer.Deserialize<BsonArray>(mongoAggregateQueryString);
            foreach (var el in aggElements)
            {
                docs.Add(el.AsBsonDocument);
            }
            aggregatePipeline = docs.ToArray<BsonDocument>();
            //Build a Pipeline out of multiple single Bson documents
            //BsonDocument matchBsonDoc = BsonDocument.Parse("{ \"$match\" : { \"City\" : \"Vienna\" } }");
            //aggregatePipeline = new BsonDocument[]
            //{
            //  matchBsonDoc,
            //  new BsonDocument { { "$sort", new BsonDocument("CheckPoint", 1) } }
            //};

            var aggregateResults = col.Aggregate(aggregatePipeline);
            foreach (var item in aggregateResults.ToList<MongoItem>())
                Console.WriteLine($"{item.DemoUser.FirstName} {item.DemoUser.LastName}");
            #endregion

            var query1 = query.Where(cityExpressionVienna).OrderBy(orderByDate);
            var query2 = query.Where(cityExpressionVienna).OrderByDescending(orderByDate);
            var query3 = query.Where(cityExpressionVienna.AndAlso(containsTagFemale)).OrderBy(orderByDate);
            var query4 = query.Where(cityExpressionGraz.AndAlso(containsTagFemale).AndAlso(containsUserID)).OrderBy(orderByDate);
            var query5 = query.Where(cityExpressionVienna.AndAlso(isWithinDateRange)).OrderBy(orderByDate);

            List<MongoItem> queryResult = null;
            queryResult = query1.ToList<MongoItem>();
            queryResult.Assert(new string[] { "Andreas Pollak", "Nina Huber", "Michaela Bauer" });
            queryResult = query2.ToList<MongoItem>();
            queryResult.Assert(new string[] { "Michaela Bauer", "Nina Huber", "Andreas Pollak" });
            queryResult = query3.ToList<MongoItem>();
            queryResult.Assert(new string[] { "Nina Huber", "Michaela Bauer" });
            queryResult = query4.ToList<MongoItem>();
            queryResult.Assert(new string[] { "Martina Uhlig" });
            queryResult = query5.ToList<MongoItem>();
            queryResult.Assert(new string[] { "Andreas Pollak", "Nina Huber" });
        }


        /// <summary>
        /// Create some Test Data in Memory
        /// </summary>
        /// <returns></returns>
        public List<MongoItem> CreateTestData()
        {
            List<MongoItem> result = new List<MongoItem>();
            MongoItem newItem;

            newItem = new MongoItem("Andreas", "Pollak", "Vienna", "ModeA", "2017:04:03 14:00:00", 48.080, 16.140, new string[] { "person", "male" }, new string[] { "apollak", "pollaka" });
            result.Add(newItem);
            newItem = new MongoItem("Nina", "Huber", "Vienna", "ModeA", "2017:04:03 15:00:00", 48.085, 16.145, new string[] { "person", "female" }, new string[] { "ninah", "hnina" });
            result.Add(newItem);
            newItem = new MongoItem("Michaela", "Bauer", "Vienna", "ModeB", "2017:04:03 16:00:00", 48.090, 16.150, new string[] { "person", "female" }, new string[] { "michib", "bmichi" });
            result.Add(newItem);
            newItem = new MongoItem("Karl", "Sarg", "Graz", "ModeB", "2017:04:03 19:00:00", 48.095, 16.155, new string[] { "person", "male" }, new string[] { "ksarg", "sargk" });
            result.Add(newItem);
            newItem = new MongoItem("Martina", "Uhlig", "Graz", "ModeC", "2017:04:02 18:00:00", 48.100, 16.160, new string[] { "person", "female" }, new string[] { "martinau", "umartina" });
            result.Add(newItem);
            newItem = new MongoItem("Susan", "Maier", "Graz", "ModeC", "2017:04:02 17:00:00", 48.105, 16.165, new string[] { "person", "female" }, new string[] { "susanm", "msusan" });
            result.Add(newItem);
            newItem = new MongoItem("Karin", "Janos", "Graz", "ModeD", "2017:04:05 09:00:00", 48.110, 16.170, new string[] { "person", "female" }, new string[] { "karinj", "jkarin" });
            result.Add(newItem);

            return result;
        }

        /// <summary>
        /// Write a List of MongoItem-Instance to a MongoDB Collection
        /// </summary>
        /// <param name="collection"></param>
        /// <param name="items"></param>
        /// <returns></returns>
        public async Task AddItemsToMongoDBCollection(IMongoCollection<MongoItem> collection, List<MongoItem> items)
        {
            foreach (MongoItem i in items)
                await collection.InsertOneAsync(i);
        }

    }
}
