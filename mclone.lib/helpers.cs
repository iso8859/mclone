using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace mclone.lib
{
    public static class helpers
    {
        static public BsonDocument FindById(this IMongoCollection<BsonDocument> coll, BsonValue id) => coll.Find(Builders<BsonDocument>.Filter.Eq("_id", id)).FirstOrDefault();

        public static bool SameAs(this BsonDocument source, BsonDocument other, params string[] ignoreFields)
        {
            var elements = source.Elements.Where(x => !ignoreFields.Contains(x.Name)).ToArray();
            if (elements.Length == 0 && other.Elements.Where(x => !ignoreFields.Contains(x.Name)).Count() > 0) return false;
            foreach (BsonElement element in source.Elements)
            {
                if (ignoreFields.Contains(element.Name)) continue;
                if (!other.Names.Contains(element.Name)) return false;
                BsonValue value = element.Value;
                BsonValue otherValue = other[element.Name];
                if (!value.SameAs(otherValue)) return false;
            }
            return true;
        }

        public static bool SameAs(this BsonValue value, BsonValue otherValue)
        {
            if (value.IsBsonDocument)
            {
                if (!otherValue.IsBsonDocument) return false;
                if (!value.AsBsonDocument.SameAs(otherValue.AsBsonDocument)) return false;
            }
            else if (value.IsBsonArray)
            {
                if (!otherValue.IsBsonArray) return false;
                if (value.AsBsonArray.Count != otherValue.AsBsonArray.Count) return false;
                var array = value.AsBsonArray.OrderBy(x => x).ToArray();
                var otherArray = otherValue.AsBsonArray.OrderBy(x => x).ToArray();
                return !array.Where((t, i) => !t.SameAs(otherArray[i])).Any();
            }
            else return value.Equals(otherValue);
            return true;
        }
    }
}
