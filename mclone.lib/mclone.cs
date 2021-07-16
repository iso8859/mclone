using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace mclone.lib
{
    public class MongoObject
    {
        public string Name { get; set; }
        public bool Include { get; set; } = true;
        [BsonExtraElements]
        [BsonIgnoreIfNull]
        public BsonDocument CatchAll { get; set; }
        public override string ToString()
        {
            return this.ToJson();
        }

    }
    public class Collection : MongoObject
    {
        // You can change destination name
        public string DestinationName { get; set; }
        // Name of the sequence field, needed to synchronize updated records
        public string SequenceField { get; set; }
        // If true will refresh all records. Usefull when using collection that can be updated with no sequence filed.
        public bool Force { get; set; } = false;
        // Can only add or update record, don't delete records
        public bool OnlyAdd { get; set; } = false;
        // If true will drop destination collection.
        public bool Drop { get; set; } = false;
        public bool Verbose { get; set; } = true;
        public int BatchSize { get; set; } = 100;
    }

    public class Database : MongoObject
    {
        // You can change destination name
        public string DestinationName { get; set; }
        public List<Collection> Collections { get; set; }
        public Collection this[string name] { get => Collections.Find(_ => _.Name == name); }

        public async Task FillCollectionsAsync(IMongoClient client)
        {
            Collections = new List<Collection>();
            var db = client.GetDatabase(Name);
            foreach (string collectionName in await db.ListCollectionNames().ToListAsync())
            {
                Collections.Add(new Collection() { Name = collectionName, DestinationName = collectionName });
            }
        }

        static async Task<Dictionary<BsonValue, BsonValue>> GetIdsAsync(IMongoCollection<BsonDocument> c, Collection settings)
        {
            bool sequence = !string.IsNullOrEmpty(settings.SequenceField);
            Dictionary<BsonValue, BsonValue> result = new();
            var bsonp = Builders<BsonDocument>.Projection;
            var projection = bsonp.Include("_id");

            if (sequence)
            {
                projection = projection.Include(settings.SequenceField);
                foreach (BsonDocument doc in await c.Find(_ => true).Project(projection).ToListAsync())
                    result[doc["_id"]] = doc[settings.SequenceField];
            }
            else
            {
                foreach (BsonDocument doc in await c.Find(_ => true).Project(projection).ToListAsync())
                    result[doc["_id"]] = 0;
            }

            return result;
        }

        static public async Task SyncAsync(IMongoClient srcClient, Database src, IMongoClient dstClient)
        {
            foreach (Collection srcCollection in src.Collections)
            {
                if (srcCollection.Include)
                {
                    var bsonf = Builders<BsonDocument>.Filter;
                    bool sequence = !string.IsNullOrEmpty(srcCollection.SequenceField);
                    Dictionary<BsonValue, BsonValue> srcIds = await GetIdsAsync(srcClient.GetDatabase(src.Name).GetCollection<BsonDocument>(srcCollection.Name), srcCollection);
                    Dictionary<BsonValue, BsonValue> destIds = await GetIdsAsync(dstClient.GetDatabase(src.DestinationName).GetCollection<BsonDocument>(srcCollection.DestinationName), srcCollection);
                    HashSet<BsonValue> existingIds = new();
                    // Manage identical ids
                    foreach (BsonValue dest in destIds.Keys)
                    {
                        if (srcIds.ContainsKey(dest)) // If destination existe remove source from srcIds
                        {
                            bool needUpdate = sequence; // except if we have a sequence field (needUpdate == true)
                            if (needUpdate)
                                needUpdate = !srcIds[dest].Equals(destIds[dest]); // If values are identical remove (needUpdate == false)
                            if (!needUpdate)
                            {
                                existingIds.Add(dest); // Keep this id for Force flag
                                srcIds.Remove(dest); // It exists in source and destination, remove it.
                            }
                        }
                    }
                    // remove destIds that have been found in srcIds
                    foreach (BsonValue val in existingIds)
                        destIds.Remove(val);

                    // The record we need to refresh
                    if (srcCollection.Force)
                    {
                        while (existingIds.Count > 0)
                        {
                            BsonArray batch = new();
                            int i = srcCollection.BatchSize;
                            while (existingIds.Count > 0 && i > 0)
                            {
                                BsonValue val = existingIds.FirstOrDefault();
                                batch.Add(val);
                                existingIds.Remove(val);
                                destIds.Remove(val); // Remove from dest, it has been processed
                                i--;
                            }
                            DateTime start = DateTime.Now;
                            List<BsonDocument> s = await srcClient.GetDatabase(src.Name).GetCollection<BsonDocument>(srcCollection.Name).Find(bsonf.In("_id", batch)).ToListAsync();
                            if (srcCollection.Verbose)
                                Console.WriteLine($"Read {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms {src.Name}.{srcCollection.Name}");
                            start = DateTime.Now;
                            foreach (BsonDocument doc in s)
                                await dstClient.GetDatabase(src.DestinationName).GetCollection<BsonDocument>(srcCollection.DestinationName).ReplaceOneAsync(bsonf.Eq("_id", doc["_id"]), doc);
                            if (srcCollection.Verbose)
                                Console.WriteLine($"Replace {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms {src.DestinationName}.{srcCollection.DestinationName}");
                        }
                    }
                    else if (srcCollection.Verbose && existingIds.Count>0)
                        Console.WriteLine($"Ignore {existingIds.Count} records in {src.Name}.{srcCollection.Name}");

                    // The new records
                    while (srcIds.Count > 0) // There is some src that does not exists or are different
                    {
                        BsonArray batch = new();
                        int i = srcCollection.BatchSize;
                        foreach (var val in srcIds)
                        {
                            batch.Add(val.Key);
                            destIds.Remove(val.Key);
                            i--;
                            if (i == 0)
                                break;
                        }
                        foreach (BsonValue val2 in batch)
                            srcIds.Remove(val2);

                        DateTime start = DateTime.Now;
                        List<BsonDocument> s = await srcClient.GetDatabase(src.Name).GetCollection<BsonDocument>(srcCollection.Name).Find(bsonf.In("_id", batch)).ToListAsync();
                        if (srcCollection.Verbose)
                            Console.WriteLine($"Read {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms {src.Name}.{srcCollection.Name}");
                        start = DateTime.Now;
                        await dstClient.GetDatabase(src.DestinationName).GetCollection<BsonDocument>(srcCollection.DestinationName).InsertManyAsync(s);
                        if (srcCollection.Verbose)
                            Console.WriteLine($"Create {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms {src.DestinationName}.{srcCollection.DestinationName}");
                    }

                    // Remaining destIds are to be deleted
                    if (!srcCollection.OnlyAdd && destIds.Count > 0)
                    {
                        Console.WriteLine($"Delete {destIds.Count} records.");
                        while (destIds.Count > 0)
                        {
                            BsonArray batch = new();
                            int i = srcCollection.BatchSize;
                            foreach (var val in destIds)
                            {
                                batch.Add(val.Key);
                                i--;
                                if (i == 0)
                                    break;
                            }
                            foreach (BsonValue val2 in batch)
                                destIds.Remove(val2);

                            await dstClient.GetDatabase(src.DestinationName).GetCollection<BsonDocument>(srcCollection.DestinationName).DeleteManyAsync(bsonf.In("_id", batch));
                        }
                    }
                }
                else
                    Console.WriteLine($"Ignore {src.Name}.{srcCollection.Name}");
            }
        }
    }

    public class Server : MongoObject
    {
        public List<Database> Databases { get; set; }
        public Database this[string name] { get => Databases.Find(_ => _.Name == name); }
        public async Task FillDatabasesAsync(string uri)
        {
            var client = new MongoClient(uri);
            Databases = new List<Database>();
            foreach (string dbName in await client.ListDatabaseNames().ToListAsync())
            {
                var db = new Database() { Name = dbName, DestinationName = dbName };
                await db.FillCollectionsAsync(client);
                Databases.Add(db);
            }
        }

    }

    public class Config : MongoObject
    {
        public string SourceUri { get; set; } = "mongodb://127.0.0.1";
        public string DestinationUri { get; set; } = "mongodb://127.0.0.1";
        public Server SourceServer { get; set; } = new() { Name = "source server" };
        public string ToJsonString() => this.ToJson(new() { Indent = true });
        public static Config Parse(string jsonString) => MongoDB.Bson.Serialization.BsonSerializer.Deserialize<Config>(jsonString);
        public async Task FillAsync()
        {
            await SourceServer.FillDatabasesAsync(SourceUri);
            if (SourceServer["admin"] != null) SourceServer["admin"].Include = false;
            if (SourceServer["config"] != null) SourceServer["config"].Include = false;
            if (SourceServer["local"] != null) SourceServer["local"].Include = false;
        }

        // Only add missing records
        public async Task SyncAsync()
        {
            var srcClient = new MongoClient(SourceUri);
            var dstClient = new MongoClient(DestinationUri);
            foreach (Database srcDb in SourceServer.Databases)
            {
                if (srcDb.Include)
                {
                    // Get all ids in destination
                    await Database.SyncAsync(srcClient, srcDb, dstClient);
                }
                else
                    Console.WriteLine($"Ignore database {srcDb.Name}");
            }
        }
    }
}
