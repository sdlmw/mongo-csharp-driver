using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Benchmarking.Benchmarks
{
    internal class FindOneBenchmark : AbstractFindBenchmark<BsonDocument>
    {
        public FindOneBenchmark(string resourcePath, int numInternalIterations)
            : base("TestFindOneByID", resourcePath, numInternalIterations)
        { }

        public override void Run()
        {
            for (int i = 0; i < _numInternalIterations; i++)
            {
                _collection.Find(new BsonDocument("_id", i)).First();
            }
        }
    }
}
