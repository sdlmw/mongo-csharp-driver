using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Benchmarking.Benchmarks
{
    internal abstract class AbstractInsertBenchmark<TDocument> : AbstractMongoBenchmark
    {
        private IMongoDatabase _database;
        private readonly string _resourcePath;
        private int _fileLength;
        protected IMongoCollection<TDocument> _collection;
        protected DocumentResource<TDocument> _resource;
        protected readonly int _numInternalIterations;

        protected AbstractInsertBenchmark(string name, string resourcePath, int numInternalIterations)
            : base(name)
        {
            _resourcePath = resourcePath;
            _numInternalIterations = numInternalIterations;
        }

        public override int GetBytesPerRun()
        {
            return _resource.Bytes.Length * _numInternalIterations;
        }

        public override void SetUp()
        {
            base.SetUp();
            DropDatabase();
            _collection = GetCollection<TDocument>();

            _resource = LoadDocumentResource<TDocument>(_resourcePath);
        }

        public override void Before()
        {
            DropCollection();
            CreateCollection();
        }
    }
}