using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.IO;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace Benchmarking.Benchmarks
{
    internal class BsonDecodeDocumentBenchmark<TDocument> : AbstractBsonDocumentBenchmark<TDocument>
    {
        public BsonDecodeDocumentBenchmark(string name, string resourcePath, int numInternalIterations)
            : base("Test" + name + "Decoding", resourcePath, numInternalIterations)
        {
        }

        public override void Run()
        {
            for (int i = 0; i < _numInternalIterations; i++)
            {
                using (var buffer = ByteBufferFactory.Create(new InputBufferChunkSource(BsonChunkPool.Default), _documentBytes.Length))
                {
                    buffer.Length = _documentBytes.Length;
                    buffer.SetBytes(0, _documentBytes, 0, _documentBytes.Length);
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