using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;

namespace mclone.lib
{
    public class mongoObject
    {
        public string name { get; set; }
        public bool include { get; set; } = true;
        [BsonExtraElements]
        [BsonIgnoreIfNull]
        public BsonDocument catchall { get; set; }
        public override string ToString()
        {
            return this.ToJson();
        }

    }
    public class collection : mongoObject
    {
        public bool force { get; set; } = false;
        public bool verbose { get; set; } = true;
        public int batchSize { get; set; } = 100;
    }

    public class database : mongoObject
    {
        public List<collection> collections { get; set; }
        public async Task FillCollectionsAsync(IMongoClient client)
        {
            collections = new List<collection>();
            var db = client.GetDatabase(name);
            foreach (string collectionName in await db.ListCollectionNames().ToListAsync())
            {
                collections.Add(new collection() { name = collectionName });
            }
        }
        static public async Task SyncAsync(IMongoClient srcClient, database src, IMongoClient dstClient)
        {
            foreach (collection srcCollection in src.collections)
            {
                if (srcCollection.include)
                {
                    // Get existing ids
                    HashSet<BsonValue> destIds = new HashSet<BsonValue>((await dstClient.GetDatabase(src.name).GetCollection<BsonDocument>(srcCollection.name).Find(_ => true).Project(Builders<BsonDocument>.Projection.Include("_id")).ToListAsync()).Select(_ => _["_id"]).ToList());
                    HashSet<BsonValue> srcIds = new HashSet<BsonValue>((await srcClient.GetDatabase(src.name).GetCollection<BsonDocument>(srcCollection.name).Find(_ => true).Project(Builders<BsonDocument>.Projection.Include("_id")).ToListAsync()).Select(_ => _["_id"]).ToList());
                    HashSet<BsonValue> existingIds = new HashSet<BsonValue>();
                    // Manage identical ids
                    foreach (BsonValue dest in destIds)
                    {
                        if (srcIds.Contains(dest))
                        {
                            existingIds.Add(dest);
                            srcIds.Remove(dest);
                        }
                    }
                    if (srcCollection.verbose && existingIds.Count > 0 && !srcCollection.force)
                        Console.WriteLine($"Ignore {src.name}.{srcCollection.name} : {existingIds.Count} records");
                    if (srcCollection.force)
                    {
                        while (existingIds.Count > 0)
                        {
                            BsonArray batch = new BsonArray();
                            int i = srcCollection.batchSize;
                            while (existingIds.Count > 0 && i > 0)
                            {
                                BsonValue val = existingIds.FirstOrDefault();
                                batch.Add(val);
                                existingIds.Remove(val);
                                i--;
                            }
                            DateTime start = DateTime.Now;
                            List<BsonDocument> s = await srcClient.GetDatabase(src.name).GetCollection<BsonDocument>(srcCollection.name).Find(Builders<BsonDocument>.Filter.In("_id", batch)).ToListAsync();
                            if (srcCollection.verbose)
                                Console.WriteLine($"Read {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms {src.name}.{srcCollection.name}");
                            start = DateTime.Now;
                            foreach (BsonDocument doc in s)
                                await dstClient.GetDatabase(src.name).GetCollection<BsonDocument>(srcCollection.name).ReplaceOneAsync(Builders<BsonDocument>.Filter.Eq("_id", doc["_id"]), doc);
                            if (srcCollection.verbose)
                                Console.WriteLine($"Repalace {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms {src.name}.{srcCollection.name}");
                        }
                    }
                    while (srcIds.Count > 0)
                    {
                        BsonArray batch = new BsonArray();
                        int i = srcCollection.batchSize;
                        while (srcIds.Count > 0 && i > 0)
                        {
                            BsonValue val = srcIds.FirstOrDefault();
                            batch.Add(val);
                            srcIds.Remove(val);
                            i--;
                        }
                        DateTime start = DateTime.Now;
                        List<BsonDocument> s = await srcClient.GetDatabase(src.name).GetCollection<BsonDocument>(srcCollection.name).Find(Builders<BsonDocument>.Filter.In("_id", batch)).ToListAsync();
                        if (srcCollection.verbose)
                            Console.WriteLine($"Read {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms {src.name}.{srcCollection.name}");
                        start = DateTime.Now;
                        await dstClient.GetDatabase(src.name).GetCollection<BsonDocument>(srcCollection.name).InsertManyAsync(s);
                        if (srcCollection.verbose)
                            Console.WriteLine($"Write {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms {src.name}.{srcCollection.name}");
                    }
                }
                else
                    Console.WriteLine($"Ignore {src.name}.{srcCollection.name}");
            }
        }
    }

    public class server : mongoObject
    {
        public string uri { get; set; }
        public List<database> databases { get; set; }
        [BsonExtraElements]
        public BsonDocument indexes { get; set; }
        public async Task FillDatabasesAsync()
        {
            var client = new MongoClient(uri);
            databases = new List<database>();
            foreach (string dbName in await client.ListDatabaseNames().ToListAsync())
            {
                var db = new database() { name = dbName };
                await db.FillCollectionsAsync(client);
                databases.Add(db);
            }
        }
    }

    public class config : mongoObject
    {
        public server source { get; set; }
        public server destination { get; set; }
        public async Task FillAsync()
        {
            if (source != null)
                await source.FillDatabasesAsync();
            if (destination != null)
                await destination.FillDatabasesAsync();
        }

        // Only add missing records
        public async Task SyncAsync()
        {
            var srcClient = new MongoClient(source.uri);
            var dstClient = new MongoClient(destination.uri);
            foreach (database srcDb in source.databases.FindAll(_=>_.include))
            {
                if (srcDb.include)
                {
                    // Get all ids in destination
                    await database.SyncAsync(srcClient, srcDb, dstClient);
                }
                else
                    Console.WriteLine($"ignore database {srcDb.name}");
            }
        }
    }
}
