using Microsoft.Azure.Documents;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI
{
    public class Person
    {
        public static explicit operator Person(Document doc)
        {
            Person personResult = new Person();
            personResult.id = doc.GetPropertyValue<string>("id");
            personResult.name = doc.GetPropertyValue<string>("name");
            personResult.label = doc.GetPropertyValue<string>("label");
            personResult.city = doc.GetPropertyValue<string>("city");
            /* and so on, for all the properties of Employee */
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
