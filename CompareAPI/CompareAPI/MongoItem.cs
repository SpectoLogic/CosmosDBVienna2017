using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI
{
    public class MUser
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
    }

    
    public class MongoItem
    {
        [MongoDB.Bson.Serialization.Attributes.BsonId]
        public string id { get; set; }
        public string Container { get; set; }
        public MUser UploadUser { get; set; }
        public string[] UserFlag { get; set; }
        public string[] TagList { get; set; }
    }
}
