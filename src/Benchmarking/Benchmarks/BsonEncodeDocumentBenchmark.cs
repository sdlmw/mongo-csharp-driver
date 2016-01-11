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
    internal class BsonEncodeDocumentBenchmark<TDocument> : AbstractBsonDocumentBenchmark<TDocument>
    {
        public BsonEncodeDocumentBenchmark(string name, string resourcePath, int numInternalIterations)
            : base("Test" + name + "Encoding", resourcePath, numInternalIterations)
        {
        }

        public override void Run()
        {
            for (int i = 0; i < _numInternalIterations; i++)
            {
                //using (var buffer = ByteBufferFactory.Create(new OutputBufferChunkSource(BsonChunkPool.Default), _documentBytes.Length))
                //using (var stream = new ByteBufferStream(buffer))
                using (var stream = new MemoryStream())
                using (var writer = new BsonBinaryWriter(stream))
                {
                    _serializer.Serialize(BsonSerializationContext.CreateRoot(writer), _resource.Document);
                }
            }
        }
    }
}