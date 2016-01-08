using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Benchmarking.Framework;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Benchmarking.Benchmarks
{
    internal abstract class AbstractBsonDocumentBenchmark<TDocument> : Benchmark
    {
        private readonly string _resourcePath;
        protected DocumentResource<TDocument> _resource;
        protected byte[] _documentBytes;
        protected readonly int _numInternalIterations;
        protected readonly IBsonSerializer<TDocument> _serializer = BsonSerializer.LookupSerializer<TDocument>();

        protected AbstractBsonDocumentBenchmark(string name, string resourcePath, int numInternalIterations)
        {
            Name = name;
            _resourcePath = resourcePath;
            _numInternalIterations = numInternalIterations;
        }

        public override string Name { get; }

        public override void SetUp()
        {
            _resource = LoadDocumentResource<TDocument>(_resourcePath);
            _documentBytes = _resource.Document.ToBson();
        }

        public override int GetBytesPerRun()
        {
            return _resource.Bytes.Length * _numInternalIterations;
        }
    }
}
