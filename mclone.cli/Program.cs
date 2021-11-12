using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using Spectre.Console;
using System;
using System.Threading.Tasks;

namespace mclone
{
    class Program
    {
        public class BestPracticesDemo
        {
            public ObjectId _id { get; set; }
            [BsonElement("_s")]
            public long sequence { get; set; }
            [BsonExtraElements]
            public BsonDocument Properties { get; set; }
        }
        static async Task Test()
        {
            lib.Config config = new()
            {
                SourceUri = System.Environment.GetEnvironmentVariable("MONGODB"),
                DestinationUri = System.Environment.GetEnvironmentVariable("mclonedest")
            };
            await config.FillAsync();
            config.SourceServer["Chorus"]["Piste"].Force = true;
            config.SourceServer["Chorus"]["seq"].Force = true;
            await config.SyncAsync();
            // Console.WriteLine(config.ToJsonString());
        }

        public static readonly string json = "mclone.json";
        public static readonly string arg_create = "create";
        public static readonly string arg_config = "config";
        public static readonly string arg_populate = "populate";
        public static readonly string arg_sync = "sync";
        public static readonly string arg_render = "render";
        static async Task MainAsync()
        {
            Console.WriteLine(@"mclone.exe version 1.3 2011111202 https://github.com/iso8859/mclone");
            SuperSimpleParser.CommandLineParser clp = SuperSimpleParser.CommandLineParser.Parse(Environment.CommandLine);
            if (clp.args.Count == 0 || clp.GetBool("help"))
            {
                AnsiConsole.Write(new FigletText("mclone").LeftAligned());
                Console.WriteLine(@"
Copy or Sync two MongoDB server, databases, collections, indexes.
Works in 4 precise context
A) Destination is empty or use Force or Drop flag.
B) Collection are never updated, only add or remove records.
C) Collection contains a 'sequence' field, modified on each update.
D) Copy if you use OnlyAdd flag.

4 steps to create your first synchronisation.

1. Create an empty json file.
> mclone.exe -create [-config jsonFile]

2. Edit the json file and set the two serveur uri
> notepad.exe mclone.json

3. Execute the populate function to get source server collection map.
> mclone.exe -populate [-config jsonFile] [-render]

4. Edit the json file if you want to exclude some databases or collections. Use Include flag.
No need to edit it if you want all default options = basic synchronisation
Force collection synchro with Force = true or ForceAllCollections = true.
Set sequence field with SequenceField = 'fieldName'.
Avoid record deletion with OnlyAdd = true or OnlyAddAllCollections = true.
> mclone.exe -sync [-config jsonFile]

To see current config summary
> mclone.exe -render [-config jsonFile]

More infos on github.
");
            }
            else
            {
                // --config
                string jsonFile = clp.GetString(arg_config, json);
                if (clp.GetBool(arg_create))
                {
                    if (!System.IO.File.Exists(jsonFile))
                    {
                        System.IO.File.WriteAllText(jsonFile, (new lib.Config()).ToJsonString());
                        Console.WriteLine($"{jsonFile} created.");
                    }
                    else
                        Console.Error.WriteLine($"File {jsonFile} already exists.");
                }
                else
                {
                    if (System.IO.File.Exists(jsonFile))
                    {
                        try
                        {
                            lib.Config config = lib.Config.Parse(System.IO.File.ReadAllText(jsonFile));
                            // --render
                            if (clp.GetBool(arg_render))
                            {
                                AnsiConsole.Write(config.Render());
                            }
                            // --populate
                            if (clp.GetBool(arg_populate))
                            {
                                await config.FillAsync();
                                System.IO.File.WriteAllText(jsonFile, config.ToJsonString());
                                Console.Error.WriteLine($"File {jsonFile} populated.");
                            }
                            // --sync
                            else if (clp.GetBool(arg_sync))
                            {
                                await config.SyncAsync();
                            }
                        }
                        catch (Exception ex)
                        {
                            var e = ex;
                            while (e != null)
                            {
                                Console.Error.WriteLine(e.Message);
                                Console.Error.WriteLine(e.StackTrace);
                                e = ex.InnerException;
                            }
                        }
                    }
                    else
                        Console.Error.WriteLine($"Can't find config file.");
                }
            }
        }
        static void Main(string[] args)
        {
            //var client = new MongoDB.Driver.MongoClient();
            //// foreach (var db in client.ListDatabaseNames().ToList())
            //{
            //    var db = "mq";
            //    foreach (var ci in client.GetDatabase(db).ListCollections().ToList())
            //    {
            //        Console.WriteLine($"{db}:{ci.ToJson()}");
            //    }
            //    foreach (var collection in client.GetDatabase(db).ListCollectionNames().ToList())
            //    {
            //        var index = client.GetDatabase(db).GetCollection<BsonDocument>(collection).Indexes.List().ToList();
            //        Console.WriteLine($"{db}:{collection}:{index.ToJson()}");
            //    }
            //}
            Task.WaitAll(Task.Run(async () => await MainAsync()));
#if DEBUG
            //Console.ReadLine();
#endif
            Console.WriteLine();
        }
    }
}
