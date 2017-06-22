using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CompareAPI
{
    public enum Race
    {
        Human,
        Hobbit,
        Elf,
        Orc,
        Dwarf,
        Dragon
    }


    public class PersonEntity : TableEntity
    {
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public int Age { get; set; }
        public Race Race
        {
            get
            {
                return (Race)Enum.Parse(typeof(Race), this.PartitionKey);
            }
            set { this.PartitionKey = value.ToString(); }
        }
        public double Energy { get; set; }

        public void CreateRowKey()
        {
            this.RowKey = $"{this.FirstName}_{this.LastName}";
        }
    }
}
