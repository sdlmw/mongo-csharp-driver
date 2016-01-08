using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace Benchmarking.Benchmarks
{
    internal abstract class AbstractGridFSBenchmark : AbstractMongoBenchmark
    {
        private readonly string _resourcePath;
        protected IMongoDatabase _database;
        protected IGridFSBucket _bucket;
        protected byte[] _bytes;

        protected AbstractGridFSBenchmark(string name, string resourcePath)
            : base(name)
        {
            _resourcePath = resourcePath;
        }

        public override void SetUp()
        {
            base.SetUp();

            DropDatabase();
            _bucket = new GridFSBucket(Database);
            _bytes = File.ReadAllBytes(_resourcePath);
        }

        public override int GetBytesPerRun()
        {
            return _bytes.Length;
        }
    }
}
