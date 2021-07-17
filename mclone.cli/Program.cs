using MongoDB.Bson;
using MongoDB.Driver;
using Spectre.Console;
using System;
using System.Threading.Tasks;

namespace mclone
{
    class Program
    {
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
            // Console.WriteLine(config.ToJson());
        }

        public static readonly string json = "mclone.json";
        public static readonly string arg_create = "create";
        public static readonly string arg_config = "config";
        public static readonly string arg_populate = "populate";
        public static readonly string arg_sync = "sync";
        public static readonly string arg_render = "render";
        static async Task MainAsync()
        {
            Console.WriteLine(@"mclone.exe version 1.1 https://github.com/iso8859/mclone");
            SuperSimpleParser.CommandLineParser clp = SuperSimpleParser.CommandLineParser.Parse(Environment.CommandLine);
            if (clp.args.Count==0 || clp.GetBool("help"))
            {
                Console.WriteLine(@"
Copy or Sync two MongoDB server, databases, collections.
Works in 3 precise context
A) Destination is empty or use collection drop flag
B) Collection are never updated, only add or remove records
C) Collection contains a 'sequence' field, modified on each update

1. Create an empty json file.
mclone.exe -create [-config jsonFile]

2. Edit the json file settings the two serveur uri

3. Execute the populate function to get each server config and check connection string are working.
mclone.exe -populate [-config jsonFile] [-render]

4. Edit the json file if you want to exclude some databases or collections. Use Include flag.
Force collection synchro with Force = true or ForceAllCollections = true.
Set sequence field with SequenceField = 'fieldName'.
Avoid record deletion with OnlyAdd = true or OnlyAddAllCollections = true.
mclone.exe -sync [-config jsonFile]

To see current config summary
mclone.exe -render [-config jsonFile]
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
                                AnsiConsole.Render(config.Render());
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
                        catch(Exception ex)
                        {
                            var e = ex;
                            while (e!=null)
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
            Task.WaitAll(Task.Run(async () => await MainAsync()));
#if DEBUG
            //Console.ReadLine();
#endif
            Console.WriteLine();
        }
    }
}
