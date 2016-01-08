using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace Benchmarking.Benchmarks
{
    internal class InsertManyBenchmark : AbstractInsertBenchmark<BsonDocument>
    {
        private List<BsonDocument> _documents;
        public InsertManyBenchmark(string name, string resourcePath, int numDocuments)
            : base("Test" + name + "DocBulkInsert", resourcePath, numDocuments)
        { }

        public override void SetUp()
        {
            base.SetUp();
            _documents = new List<BsonDocument>();
            for (int i = 0; i < _numInternalIterations; i++)
            {
                _documents.Add((BsonDocument)_resource.Document.DeepClone());
            }
        }

        public override void Before()
        {
            base.Before();
            foreach (var doc in _documents)
            {
                doc.Remove("_id");
            }
        }

        public override void Run()
        {
            _collection.InsertMany(_documents);
        }
    }
}
