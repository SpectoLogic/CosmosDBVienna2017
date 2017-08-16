using Microsoft.Azure.Documents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI
{
    /// <summary>
    /// Simple Person Entity
    /// Used by: ChangeFeedDemo and GeoDistributionDemo
    /// </summary>
    public class Person
    {
        public static explicit operator Person(Document doc)
        {
            Person personResult = new Person()
            {
                id = doc.GetPropertyValue<string>("id"),
                name = doc.GetPropertyValue<string>("name"),
                label = doc.GetPropertyValue<string>("label"),
                city = doc.GetPropertyValue<string>("city")
            };
            return personResult;
        }
        public Person()
        {
            id = Guid.NewGuid().ToString("D");
        }
        public string id { get; set; }
        public string name { get; set; }
        public string label { get; set; }
        public string city { get; set; }
    }
}
