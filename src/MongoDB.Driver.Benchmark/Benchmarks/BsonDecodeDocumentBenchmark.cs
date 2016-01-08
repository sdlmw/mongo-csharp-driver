using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace MongoDB.Driver.Benchmark.Benchmarks
{
    internal class BsonDecodeDocumentBenchmark : AbstractBsonDocumentBenchmark
    {
        public BsonDecodeDocumentBenchmark(string name, string resourcePath)
            : base(name + " BSON Decoding", resourcePath)
        {
        }

        public override void Run()
        {
            using (var buffer = ByteBufferFactory.Create(new InputBufferChunkSource(BsonChunkPool.Default), _documentBytes.Length))
            {
                buffer.Length = _documentBytes.Length;
                buffer.SetBytes(0, _documentBytes, 0, _documentBytes.Length);
                for (int i = 0; i < _numInternalIterations; i++)
                {
                    using (var stream = new ByteBufferStream(buffer))
                    using (var reader = new BsonBinaryReader(stream))
                    {
                        _serializer.Deserialize(BsonDeserializationContext.CreateRoot(reader));
                    }
                }
            }
        }
    }
}
