using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI.TableDemo
{
    public class TableProperty
    {
        [JsonProperty(PropertyName ="$t")]
        public string t { get; set; }
        [JsonProperty(PropertyName ="$v")]
        public string v { get; set; }
    }
    public class PersonTableEntry
    {
        public TableProperty FirstName { get; set; }
        public TableProperty LastName { get; set; }
        public TableProperty Age { get; set; }
        public TableProperty Energy { get; set; }
    }
}
