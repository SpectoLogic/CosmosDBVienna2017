using Microsoft.Azure.Documents.Spatial;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver.GeoJsonObjectModel;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI.MongoDBDemo
{
    /// <summary>
    /// A more complex entity to try various queries on the MongoDB.
    /// </summary>
    public class MongoItem
    {
        public MongoItem()
        {
            this.id = Guid.NewGuid().ToString("D");
        }
        public MongoItem(string firstName, string lastName, 
                         string container,string mode, 
                         string checkpoint,
                         double longitude, double latitude,
                         string[] tags, string[]userids) : this()
        {
            this.DemoUser = new MUser() { FirstName = firstName, LastName = lastName };
            this.City = container;
            this.Mode = mode;
            this.CheckPoint = DateTimeOffset.ParseExact(checkpoint, "yyyy:MM:dd HH:mm:ss", null);
            this.LocationMongoDB = new MongoDB.Driver.GeoJsonObjectModel.GeoJsonPoint<MongoDB.Driver.GeoJsonObjectModel.GeoJson2DGeographicCoordinates>(new MongoDB.Driver.GeoJsonObjectModel.GeoJson2DGeographicCoordinates(longitude, latitude));
            this.LocationCosmosDB = new Microsoft.Azure.Documents.Spatial.Point(longitude, latitude);
            this.UserList = userids;
            this.TagList = tags;
        }
        [MongoDB.Bson.Serialization.Attributes.BsonId]
        public string id { get; set; }
        public string City { get; set; }
        public string Mode { get; set; }

        public GeoJsonPoint<GeoJson2DGeographicCoordinates> LocationMongoDB { get; set; }
        public Point LocationCosmosDB { get; set; }

        public MUser DemoUser { get; set; }

        public string CheckPointUTC { get; set; }
        /// <summary>
        /// Use this property to set/retrieve DateTime where the picture was taken
        /// to get the local time call: GetLocalTime() on the result.
        /// </summary>
        [JsonIgnore]
        public DateTimeOffset CheckPoint
        {
            get
            {
                if (CheckPointUTC == null) return DateTimeOffset.MinValue;
                return DateTimeOffset.Parse(this.CheckPointUTC); // UTC Based Universal sortable date/time pattern ("https://msdn.microsoft.com/en-us/library/az4se3k1(v=vs.110).aspx").
            }
            set
            {
                this.CheckPointUTC = value.ToString("u"); // UTC Based Universal sortable date/time pattern ("https://msdn.microsoft.com/en-us/library/az4se3k1(v=vs.110).aspx").
            }
        }

        public string[] UserList { get; set; }
        public string[] TagList { get; set; }
    }
}
