using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Benchmarking.Framework;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace Benchmarking.Benchmarks
{
    internal abstract class AbstractFindBenchmark<TDocument> : AbstractMongoBenchmark
    {
        private IMongoDatabase _database;
        private readonly string _resourcePath;
        private DocumentResource<BsonDocument> _resource;
        protected readonly int _numInternalIterations;
        protected IMongoCollection<TDocument> _collection;

        protected AbstractFindBenchmark(string name, string resourcePath, int numInternalIterations)
            : base(name)
        {
            _resourcePath = resourcePath;
            _numInternalIterations = numInternalIterations;
        }

        public override void SetUp()
        {
            base.SetUp();
            DropDatabase();

            _collection = GetCollection<TDocument>();

            _resource = LoadDocumentResource<BsonDocument>(_resourcePath);

            InsertCopiesOfDocument(_resource.Document, _numInternalIterations);
        }

        public override int GetBytesPerRun()
        {
            return _resource.Bytes.Length * _numInternalIterations;
        }

        private void InsertCopiesOfDocument(BsonDocument document, int numCopiesToInsert)
        {
            var collection = GetCollection<BsonDocument>();
            for (int i = 0; i < numCopiesToInsert; i++)
            {
                document["_id"] = i;
                collection.InsertOne(document);
            }
        }
    }
}
