using MongoDB.Bson;
using System;
using System.Threading.Tasks;

namespace mclone
{
    class Program
    {
        static async Task MainAsync(string[] args)
        {
            lib.config config = new lib.config()
            {
                source = new lib.server() { uri = System.Environment.GetEnvironmentVariable("MONGODB") },
                destination = new lib.server() { uri = System.Environment.GetEnvironmentVariable("mclonedest") }
            };           
            await config.FillAsync();
            config.source.databases.Find(_ => _.name == "admin").include = false;
            config.source.databases.Find(_ => _.name == "config").include = false;
            config.source.databases.Find(_ => _.name == "local").include = false;
            config.source.databases.Find(_ => _.name == "Chorus").collections.Find(_ => _.name == "Piste").force = true;
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
