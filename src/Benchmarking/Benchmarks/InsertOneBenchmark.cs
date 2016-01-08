using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace Benchmarking.Benchmarks
{
    internal class InsertOneBenchmark : AbstractInsertBenchmark<BsonDocument>
    {
        public InsertOneBenchmark(string name, string resourcePath, int numInternalIterations)
            : base("Test" + name + "DocInsertOne", resourcePath, numInternalIterations)
        { }

        public override void Run()
        {
            for (int i = 0; i < _numInternalIterations; i++)
            {
                _resource.Document.Remove("_id");
                _collection.InsertOne(_resource.Document);
            }
        }
    }
}
