using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Threading.Tasks;

namespace mclone
{
    class Program
    {

        static async Task _Main()
        {

            IMongoClient client = new MongoClient("mongodb://host:port/");
            IMongoDatabase database = client.GetDatabase("Chorus");
            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>("log");

            // Created with Studio 3T, the IDE for MongoDB - https://studio3t.com/

            BsonDocument filter = new BsonDocument();

            filter.Add("path", "transverses/ajouter/fichier");

            BsonDocument projection = new BsonDocument();

            projection.Add("DT", 1.0);

            var options = new FindOptions<BsonDocument>()
            {
                Projection = projection
            };

            using (var cursor = await collection.FindAsync(filter, options))
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    foreach (BsonDocument document in batch)
                    {
                        Console.WriteLine(document.ToJson());
                    }
                }
            }
        }

        static async Task _Main2(string[] args)
        {
            var db = new MongoClient(System.Environment.GetEnvironmentVariable("MONGODB")).GetDatabase("Chorus");
            foreach (BsonDocument document in await db.GetCollection<BsonDocument>("log")
                .Find(Builders<BsonDocument>.Filter.Eq("path", "transverses/ajouter/fichier"))
                .Project(Builders<BsonDocument>.Projection.Include("DT"))
                .ToListAsync()
                )
            {
                Console.WriteLine(document.ToJson());
            }
        }
        static async Task MainAsync(string[] args)
        {
            lib.config config = new lib.config()
            {
                source = new lib.server() { uri = System.Environment.GetEnvironmentVariable("MONGODB") },
                destination = new lib.server() { uri = System.Environment.GetEnvironmentVariable("mclonedest") }
            };
            await config.FillAsync();
            config.source["admin"].include = false;
            config.source["config"].include = false;
            config.source["local"].include = false;
            config.source["Chorus"]["Piste"].force = true;
            await config.SyncAsync();
            // Console.WriteLine(config.ToJson());
        }
        static void Main(string[] args)
        {
            Task.WaitAll(Task.Run(async () => await MainAsync(args)));
            Console.ReadLine();
        }
    }
}
