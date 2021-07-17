# mclone
MongoDB clone CLI, inspired from rclone.



```
mclone.exe version 1.1 https://github.com/iso8859/mclone
                     _
  _ __ ___     ___  | |   ___    _ __     ___
 | '_ ` _ \   / __| | |  / _ \  | '_ \   / _ \
 | | | | | | | (__  | | | (_) | | | | | |  __/
 |_| |_| |_|  \___| |_|  \___/  |_| |_|  \___|


Copy or Sync two MongoDB server, databases, collections.
Works in 3 precise context
A) Destination is empty or use Force flag.
B) Collection are never updated, only add or remove records.
C) Collection contains a 'sequence' field, modified on each update.
D) Copy if you use OnlyAdd flag.

4 steps to create your first synchronisation.

1. Create an empty json file.
> mclone.exe -create [-config jsonFile]

2. Edit the json file and set the two serveur uri

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
```

## MongoDB best practices in C#

```
public class BestPracticesDemo
{
    public ObjectId _id { get; set; }
    [BsonElement("_s")] 
    public long sequence { get; set; }
    [BsonExtraElements]
    public BsonDocument Properties { get; set; }
}
```

1) Always add [BsonExtraElements], this make your app descending compatible with schema evolution.
2) Use sequence field if you update your record. Needed if several process access the same database. Use int if few update, long if a lot of update, DateTime if you need statistics or record monitoring.

## How to use mclone ?

**Runtime**

You need .NET Core 5.0 runtime. You can find it here https://dotnet.microsoft.com/

You can run it on Windows, Linux or MacOS

**Download last released version**

https://github.com/iso8859/mclone/releases

Unzip in any directory.

**Compile youself**
```
git clone https://github.com/iso8859/mclone.git
cd mclone/mclone.cli
dotnet build --configuration Release
cd bin/Release/net5.0
.\mclone.exe
```

**Use it from you app**

Look at usage example in Program.cs Test() function.