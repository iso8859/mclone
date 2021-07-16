using MongoDB.Bson;
using MongoDB.Driver;
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
                Source = new lib.Server() { Uri = System.Environment.GetEnvironmentVariable("MONGODB") },
                Destination = new lib.Server() { Uri = System.Environment.GetEnvironmentVariable("mclonedest") }
            };
            await config.FillAsync();
            config.Source["Chorus"]["Piste"].Force = true;
            config.Source["Chorus"]["seq"].Force = true;
            await config.SyncAsync();
            // Console.WriteLine(config.ToJson());
        }

        public static readonly string json = "mclone.json";
        public static readonly string arg_create = "create";
        public static readonly string arg_config = "config";
        public static readonly string arg_populate = "populate";
        public static readonly string arg_sync = "sync";
        static async Task MainAsync()
        {
            SuperSimpleParser.CommandLineParser clp = SuperSimpleParser.CommandLineParser.Parse(Environment.CommandLine);
            if (clp.args.Count==0 || clp.GetBool("help"))
            {
                Console.WriteLine(@"mclone.exe version 1.0\
Copy or Sync two MongoDB server.\
Works in 3 precise context
A) Destination is empty or use collection drop flag
B) Collection are never updated, only add or remove records
C) Collection contains a 'sequence' field, modified on each update
\
1. Create an empty json file.
mclone.exe -create [jsonFile]

2. Edit the json file settings the two serveur uri

3. Execute the populate function to get each server config and check connection string are working.
mclone.exe -populate [jsonFile]

4. Edit the json file if you want to exclude some databases or collections. Use Include flag.
Force collection synchro with Force = true.
Drop collection with Drop = true.
Set sequence field with SequenceField = 'fieldName'.
Avoid record deletion with OnlyAdd = true.
");
            }
            else
            {
                if (clp.args.ContainsKey(arg_create))
                {
                    string create = clp.GetString(arg_create, json);
                    if (!System.IO.File.Exists(create))
                        System.IO.File.WriteAllText(create, (new lib.Config()).ToJsonString());
                    else
                        Console.Error.WriteLine($"File {create} already exists.");
                }
                else
                {
                    string jsonFile = clp.GetString(arg_config, json);
                    if (System.IO.File.Exists(jsonFile))
                    {
                        try
                        {
                            lib.Config config = lib.Config.Parse(System.IO.File.ReadAllText(jsonFile));
                            if (clp.GetBool(arg_populate))
                            {
                                await config.FillAsync();
                                System.IO.File.WriteAllText(jsonFile, config.ToJsonString());
                            }
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
            Console.ReadLine();
        }
    }
}
