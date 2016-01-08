using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Benchmarking.Benchmarks
{
    internal class FindManyBenchmark : AbstractFindBenchmark<BsonDocument>
    {
        public FindManyBenchmark(string resourcePath, int numInternalIterations)
            : base("TestFindManyAndEmptyCursor", resourcePath, numInternalIterations)
        { }

        public override void Run()
        {
            using (var cursor = _collection.Find(Builders<BsonDocument>.Filter.Empty).ToCursor())
            {
                while (cursor.MoveNext())
                {
                    foreach (var doc in cursor.Current)
                    {
                        // just iterating... do we need to do this?
                    }
                }
            }
        }
    }
}
