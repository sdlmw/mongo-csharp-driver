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
    internal class ImportBenchmark : AbstractMongoBenchmark
    {
        private readonly IEnumerable<string> _resourcePaths;
        private IMongoCollection<RawBsonDocument> _collection;

        public ImportBenchmark(IEnumerable<string> resourcePaths)
            : base("TestJsonMultiImport")
        {
            _resourcePaths = resourcePaths.ToList();
        }

        public override void SetUp()
        {
            base.SetUp();
            _collection = GetCollection<RawBsonDocument>();
        }

        public override void Before()
        {
            base.Before();
            DropCollection();
            CreateCollection();
        }

        public override void Run()
        {
            Parallel.ForEach(_resourcePaths, resourcePath =>
            {
                using (var stream = File.OpenRead(resourcePath))
                using (var reader = new StreamReader(stream))
                {
                    var docs = new List<RawBsonDocument>();
                    string line;
                    while ((line = reader.ReadLine()) != null)
                    {
                        using (var jsonReader = new JsonReader(line))
                        {
                            var context = BsonDeserializationContext.CreateRoot(jsonReader);
                            var doc = _collection.DocumentSerializer.Deserialize(context);
                            docs.Add(doc);
                            if (docs.Count == 1000)
                            {
                                _collection.InsertMany(docs);
                                docs.Clear();
                            }
                        }
                    }

                    if (docs.Count > 0)
                    {
                        _collection.InsertMany(docs);
                    }
                }
            });
        }

        public override int GetBytesPerRun()
        {
            return 557610482;
        }
    }
}
