using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using Spectre.Console;

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
        // Do we also synchorine index ?
        public bool IncludeIndex { get; set; } = true;
        // Do not remove destination index that doesn't exist in source
        public bool OnlyAddIndex { get; set; } = false;
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
        public bool Stats { get; set; } = false;
        public int BatchSize { get; set; } = 0;

        public void Render(TreeNode root)
        {
            if (Include)
            {
                TreeNode node = null;
                if (Name != DestinationName)
                    node = root.AddNode("[green4]"+Name + "[/]=>[green3]" + DestinationName + "[/]");
                else
                    node = root.AddNode("[green4]" + Name + "[/]");

                if (!string.IsNullOrEmpty(SequenceField))
                    node.AddNode($"[lightsteelblue]SequenceField={SequenceField}[/]");
                if (Force)
                    node.AddNode("[lightsteelblue]Force[/]");
                if (OnlyAdd)
                    node.AddNode("[lightsteelblue]OnlyAdd[/]");
            }
        }

        public async Task SyncAsync(IMongoDatabase src, IMongoDatabase dest)
        {
            if (Include)
            {
                if (Drop)
                    dest.DropCollection(Name);

                var srcCollectionInfo = (await src.ListCollections().ToListAsync()).Find(_ => _["name"].AsString == Name);
                if (srcCollectionInfo != null)
                {
                    // Compare with destination
                    var destCollectionInfo = (await dest.ListCollections().ToListAsync()).Find(_ => _["name"].AsString == DestinationName);
                    if (destCollectionInfo == null)
                    {
                        // Destination doesn't exists, create it
                        // { "name" : "ipc_users", "type" : "collection", "options" : { "capped" : true, "size" : 1048576 }, "info" : { "readOnly" : false }, "idIndex" : { "v" : 2, "key" : { "_id" : 1 }, "name" : "_id_", "ns" : "mq.xxx" } }

                        if (srcCollectionInfo.Contains("options") && srcCollectionInfo["options"].IsBsonDocument)
                        {
                            BsonDocument srcOptions = srcCollectionInfo["options"].AsBsonDocument;
                            srcOptions.InsertAt(0, new BsonElement("create", DestinationName));
                            dest.RunCommand<BsonDocument>(srcOptions);
                        }
                    }
                    if (IncludeIndex)
                    {
                        // ipc_users:[{ "v" : 2, "key" : { "_id" : 1 }, "name" : "_id_", "ns" : "mq.ipc_users" }, { "v" : 2, "key" : { "action" : 1 }, "name" : "action_1", "ns" : "mq.ipc_users" }]
                        var srcIndexes = src.GetCollection<BsonDocument>(Name).Indexes.List().ToList();
                        var destIndexes = dest.GetCollection<BsonDocument>(DestinationName).Indexes.List().ToList();
                        BsonArray buildIndexes = new BsonArray();
                        List<string> names = new List<string>();
                        foreach (BsonDocument index in srcIndexes)
                        {
                            string name = index["name"].AsString;
                            names.Add(name);
                            // Does index exist in destination and is the same ?
                            BsonDocument d = destIndexes.Find(_ => _["name"] == name);
                            bool create = d == null;
                            if (!create && !d.SameAs(index, "ns"))
                            {
                                // Drop destination index and recreate it
                                dest.GetCollection<BsonDocument>(DestinationName).Indexes.DropOne(name);
                                create = true;
                            }
                            if (create)
                            {
                                index.Remove("ns");
                                buildIndexes.Add(index);
                            }
                        }
                        // Drop destination index that should not exists
                        if (!OnlyAddIndex)
                        {
                            foreach (BsonDocument index in destIndexes)
                            {
                                string name = index["name"].AsString;
                                if (!names.Contains(name))
                                    dest.GetCollection<BsonDocument>(DestinationName).Indexes.DropOne(name);
                            }
                        }
                        if (buildIndexes.Count > 0)
                        {
                            BsonDocument idxCommand = new BsonDocument()
                            {
                                {"createIndexes", DestinationName },
                                {"indexes", buildIndexes }
                            };
                            dest.RunCommand<BsonDocument>(idxCommand);
                        }
                    }
                }
            }
        }
    }

    public class Database : MongoObject
    {
        // You can change destination name
        public string DestinationName { get; set; }
        // All collections are forced
        public bool ForceAllCollections { get; set; } = false;
        public bool OnlyAddAllCollections { get; set; } = false;
        public bool VerboseAllCollections { get; set; } = false;
        public bool StatsAllCollections { get; set; } = false;
        public int BatchSizeAllCollections { get; set; } = 100;
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

        static BsonArray CreateBatch(HashSet<BsonValue> ids, int batchSize)
        {
            BsonArray result = new();
            int i = 0;
            foreach (BsonValue val in ids)
            {
                result.Add(val);
                i++;
                if (i == batchSize)
                    break;
            }
            foreach (BsonValue val in result)
                ids.Remove(val);
            return result;
        }

        static public async Task SyncAsync(IMongoClient srcClient, Database srcDb, IMongoClient dstClient)
        {
            foreach (Collection srcCollection in srcDb.Collections)
            {
                if (srcCollection.Verbose || srcDb.VerboseAllCollections)
                    Console.WriteLine("----");

                if (srcCollection.Include)
                {
                    await srcCollection.SyncAsync(srcClient.GetDatabase(srcDb.Name), dstClient.GetDatabase(srcDb.DestinationName));

                    var bsonf = Builders<BsonDocument>.Filter;
                    bool sequence = !string.IsNullOrEmpty(srcCollection.SequenceField);

                    Dictionary<BsonValue, BsonValue> srcIds = await GetIdsAsync(srcClient.GetDatabase(srcDb.Name).GetCollection<BsonDocument>(srcCollection.Name), srcCollection);
                    Dictionary<BsonValue, BsonValue> destIds = await GetIdsAsync(dstClient.GetDatabase(srcDb.DestinationName).GetCollection<BsonDocument>(srcCollection.DestinationName), srcCollection);

                    HashSet<BsonValue> createIds = new();
                    HashSet<BsonValue> forcedIds = new();
                    HashSet<BsonValue> updateIds = new();
                    HashSet<BsonValue> deleteIds = new();
                    int ignoreIds = 0;

                    foreach (var src in srcIds)
                    {
                        if (!destIds.ContainsKey(src.Key)) // If destination doesn't contain the id need to create it
                            createIds.Add(src.Key);
                        else
                        {
                            if (srcCollection.Force || srcDb.ForceAllCollections) // We force refresh existing records
                                forcedIds.Add(src.Key);
                            else if (sequence && !src.Value.Equals(destIds[src.Key])) // Smart refresh comparing sequence value
                                updateIds.Add(src.Key);
                            else
                                ignoreIds++; // Record is ok
                        }
                    }

                    if (!(srcCollection.OnlyAdd || srcDb.OnlyAddAllCollections)) // If we agree to delete records in destination
                    {
                        foreach (var dest in destIds)
                        {
                            if (!srcIds.ContainsKey(dest.Key))
                                deleteIds.Add(dest.Key); // Add to delete collection
                        }
                    }

                    bool verbose = (srcCollection.Verbose || srcDb.VerboseAllCollections);

                    if (verbose)
                        Console.WriteLine($"{srcDb.Name}.{srcCollection.Name}: Records:({srcIds.Count};{destIds.Count}) Create:{createIds.Count} Force update:{forcedIds.Count} Sequence update:{updateIds.Count} Delete:{deleteIds.Count} Ignore:{ignoreIds}");

                    int batchSize = srcCollection.BatchSize;
                    if (batchSize == 0)
                        batchSize = srcDb.BatchSizeAllCollections;
                    if (batchSize <= 0)
                        batchSize = 10;

                    DateTime startBatch = DateTime.Now;

                    await AnsiConsole.Progress()
                        .AutoRefresh(false) // Turn off auto refresh
                        .AutoClear(true)   // Do not remove the task list when done
                        .HideCompleted(true)   // Hide tasks as they are completed
                        .Columns(new ProgressColumn[]
                        {
                            new TaskDescriptionColumn(),    // Task description
                            new ProgressBarColumn(),        // Progress bar
                            new PercentageColumn(),         // Percentage
                            new RemainingTimeColumn(),      // Remaining time
                            new SpinnerColumn(),            // Spinner
                        })
                        .StartAsync(async ctx =>
                        {
                            // The records we need to refresh
                            foreach (BsonValue val in forcedIds)
                                updateIds.Add(val);
                            if (updateIds.Count > 0)
                            {
                                int count = updateIds.Count;
                                var task1 = ctx.AddTask("[green]Update[/]");
                                task1.MaxValue = count;
                                while (updateIds.Count > 0)
                                {
                                    BsonArray batch = CreateBatch(updateIds, batchSize);
                                    DateTime start = DateTime.Now;
                                    List<BsonDocument> s = await srcClient.GetDatabase(srcDb.Name).GetCollection<BsonDocument>(srcCollection.Name).Find(bsonf.In("_id", batch)).ToListAsync();
                                    //if (verbose)
                                    //    Console.WriteLine($"{srcDb.Name}.{srcCollection.Name}: Read {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms");
                                    start = DateTime.Now;
                                    foreach (BsonDocument doc in s)
                                        await dstClient.GetDatabase(srcDb.DestinationName).GetCollection<BsonDocument>(srcCollection.DestinationName).ReplaceOneAsync(bsonf.Eq("_id", doc["_id"]), doc);
                                    //if (verbose)
                                    //    Console.WriteLine($"{srcDb.DestinationName}.{srcCollection.DestinationName}: Replaced {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms");
                                    task1.Increment(batch.Count);
                                    ctx.Refresh();
                                }
                                if (verbose)
                                    Console.WriteLine($"{srcDb.DestinationName}.{srcCollection.DestinationName}: Replaced {count} records in {(DateTime.Now - startBatch)}");
                            }

                            if (createIds.Count > 0)
                            {
                                int count = createIds.Count;
                                var task1 = ctx.AddTask("[green]Create[/]");
                                task1.MaxValue = count;
                                DateTime startBatch0 = DateTime.Now;
                                // The new records
                                while (createIds.Count > 0)
                                {
                                    BsonArray batch = CreateBatch(createIds, batchSize);
                                    DateTime start = DateTime.Now;
                                    List<BsonDocument> s = await srcClient.GetDatabase(srcDb.Name).GetCollection<BsonDocument>(srcCollection.Name).Find(bsonf.In("_id", batch)).ToListAsync();
                                    //if (verbose)
                                    //    Console.WriteLine($"{srcDb.Name}.{srcCollection.Name}: Read {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms");
                                    start = DateTime.Now;
                                    await dstClient.GetDatabase(srcDb.DestinationName).GetCollection<BsonDocument>(srcCollection.DestinationName).InsertManyAsync(s);
                                    //if (verbose)
                                    //    Console.WriteLine($"{srcDb.DestinationName}.{srcCollection.DestinationName}: Created {batch.Count} records in {(DateTime.Now - start).TotalMilliseconds}ms");
                                    task1.Increment(batch.Count);
                                    ctx.Refresh();
                                }
                                if (verbose)
                                    Console.WriteLine($"{srcDb.DestinationName}.{srcCollection.DestinationName}: Created {count} records in {(DateTime.Now - startBatch0)}");
                            }

                            if (deleteIds.Count > 0)
                            {
                                int count = deleteIds.Count;
                                var task1 = ctx.AddTask("[green]Delete[/]");
                                task1.MaxValue = count;
                                DateTime startBatch0 = DateTime.Now;
                                // The new records
                                while (deleteIds.Count > 0)
                                {
                                    BsonArray batch = CreateBatch(deleteIds, batchSize);
                                    await dstClient.GetDatabase(srcDb.DestinationName).GetCollection<BsonDocument>(srcCollection.DestinationName).DeleteManyAsync(bsonf.In("_id", batch));
                                    task1.Increment(batch.Count);
                                    ctx.Refresh();
                                }
                                if (verbose)
                                    Console.WriteLine($"{srcDb.DestinationName}.{srcCollection.DestinationName}: Deleted {count} records in {(DateTime.Now - startBatch0)}");
                            }
                        });

                    if (verbose)
                        Console.WriteLine($"\r\n{srcDb.DestinationName}.{srcCollection.DestinationName}: Total operation time {(DateTime.Now - startBatch)}");

                    srcIds = null;
                    destIds = null;
                    createIds = null;
                    forcedIds = null;
                    updateIds = null;
                    deleteIds = null;
                }
                else if (srcCollection.Verbose || srcDb.VerboseAllCollections)
                    Console.WriteLine($"{srcDb.Name}.{srcCollection.Name}: Ignore");
            }
        }

        public void Render(TreeNode root)
        {
            if (Include)
            {
                TreeNode node = null;
                if (Name != DestinationName)
                    node = root.AddNode("[dodgerblue2]" + Name + "[/]=>[dodgerblue1]" + DestinationName + "[/]");
                else
                    node = root.AddNode("[dodgerblue2]" + Name + "[/]");
                if (Collections!=null)
                {
                    foreach (var collection in Collections)
                        collection.Render(node);
                }
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

        public void Render(TreeNode root)
        {
            if (Databases!=null)
            {
                foreach (var db in Databases)
                    db.Render(root);
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
            if (Include)
            {
                if (!string.IsNullOrEmpty(Name))
                    Console.WriteLine($"Start {Name} synchro.");
                var start = DateTime.Now;
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
                        Console.WriteLine($"----\r\n{srcDb.Name}");
                }
                Console.WriteLine("=====");
                Console.WriteLine($"Total time:{DateTime.Now - start}");
            }
            else if (!string.IsNullOrEmpty(Name))
                Console.WriteLine($"Ignore {Name} synchro.");
        }

        public Tree Render()
        {
            Tree root = new Tree("Config");
            TreeNode server = root.AddNode("[red]" + SourceUri + "[/]");
            if (SourceServer != null)
                SourceServer.Render(server);
            return root;
        }
    }
}
