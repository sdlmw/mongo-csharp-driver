using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace Benchmarking.Benchmarks
{
    internal class ExportBenchmark : AbstractMongoBenchmark
    {
        private readonly IReadOnlyList<string> _resourcePaths;
        private IMongoCollection<RawBsonDocument> _collection;
        private readonly string _exportDir;

        public ExportBenchmark(string exportDir, IEnumerable<string> resourcePaths)
            : base("TestJsonMultiExport")
        {
            _resourcePaths = resourcePaths.ToList();
            _exportDir = Path.Combine(exportDir, "ldjson_export");
        }

        public override int GetBytesPerRun()
        {
            return 557610482;
        }

        public override void SetUp()
        {
            base.SetUp();
            _collection = GetCollection<RawBsonDocument>();

            Import();
        }

        public override void Before()
        {
            base.Before();
            Directory.CreateDirectory(_exportDir);
        }

        public override void Run()
        {
            Parallel.For(0, 100, i =>
            {
                using (var stream = File.OpenWrite(Path.Combine(_exportDir, "LDJSON" + i)))
                using (var writer = new StreamWriter(stream))
                using (var jsonWriter = new JsonWriter(writer))
                {
                    foreach (var doc in _collection.Find(new BsonDocument("fileId", i), new FindOptions { BatchSize = 5000 }).ToEnumerable())
                    {
                        var context = BsonSerializationContext.CreateRoot(jsonWriter);
                        _collection.DocumentSerializer.Serialize(context, doc);
                        writer.WriteLine();
                    }
                }
            });
        }

        public override void After()
        {
            base.After();
            foreach (var file in Directory.GetFiles(_exportDir))
            {
                File.Delete(file);
            }
            Directory.Delete(_exportDir);
        }

        private void Import()
        {
            var importCollection = GetCollection<BsonDocument>();
            Parallel.For(0, _resourcePaths.Count, i =>
            {
                var resourcePath = _resourcePaths[i];
                using (var stream = File.OpenRead(resourcePath))
                using (var reader = new StreamReader(stream))
                {
                    var docs = new List<BsonDocument>();
                    using (var jsonReader = new JsonReader(reader))
                    {
                        while (!jsonReader.IsAtEndOfFile())
                        {
                            var context = BsonDeserializationContext.CreateRoot(jsonReader);
                            var doc = importCollection.DocumentSerializer.Deserialize(context);
                            doc["fileId"] = i;
                            docs.Add(doc);
                            if (docs.Count == 1000)
                            {
                                importCollection.InsertMany(docs);
                                docs.Clear();
                            }
                        }
                    }

                    if (docs.Count > 0)
                    {
                        importCollection.InsertMany(docs);
                    }
                }
            });

            _collection.Indexes.CreateOne(new BsonDocument("fileId", 1));
        }
    }
}
