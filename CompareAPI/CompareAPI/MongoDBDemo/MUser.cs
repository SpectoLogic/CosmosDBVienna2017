using MongoDB.Bson.Serialization.Attributes;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI.MongoDBDemo
{
    /// <summary>
    /// Entity representing some person relevant properties to demonstrate
    /// hierachy in Entities with MongoDB
    /// </summary>
    public class MUser
    {
        [BsonElement("FirstName")]
        [JsonProperty("FirstName")]
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }
}
