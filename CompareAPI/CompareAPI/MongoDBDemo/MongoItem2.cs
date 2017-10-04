using MongoDB.Driver.GeoJsonObjectModel;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace CompareAPI.MongoDBDemo
{
    public class MongoItem2
    {
        public MongoItem2()
        {
            this.id = Guid.NewGuid().ToString("D");
            this.Location = new GeoJsonPoint<GeoJson2DGeographicCoordinates>(new GeoJson2DGeographicCoordinates(48.080, 16.140));
            this.dateUploaded = DateTime.Now;
            this.DValue = 3.2;
            this.LValue = 4;
            this.TValue = new BsonTimestamp(MongoItem2.ToUnixTime(DateTime.Now));
            this.AValue = new string[] { "Hallo", "Welt", "wie", "gehts?" };
            this.Buffer = new byte[] { 12, 13, 51, 6, 225, 121, 122 };
        }

        public static DateTime FromUnixTime(long unixTime)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return epoch.AddSeconds(unixTime);
        }
        public static long ToUnixTime(DateTime date)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return Convert.ToInt64((date - epoch).TotalSeconds);
        }


        [MongoDB.Bson.Serialization.Attributes.BsonId]
        public string id { get; set; }
        public GeoJsonPoint<GeoJson2DGeographicCoordinates> Location { get; set; }
        [BsonDateTimeOptions(DateOnly =false, Kind = DateTimeKind.Utc, Representation = BsonType.DateTime)]
        public DateTime dateUploaded { get; set; }

        [BsonRepresentation(BsonType.Double)]
        public Double DValue { get; set; }
        [BsonRepresentation(BsonType.Int64)]
        public long LValue { get; set; }

        public BsonTimestamp TValue { get; set; }

        public string[] AValue { get; set; }

        [BsonRepresentation(BsonType.Binary)]
        public byte[] Buffer { get; set; }


    }
}
