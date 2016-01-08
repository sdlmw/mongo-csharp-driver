using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace Benchmarking.Framework
{
    internal abstract class Benchmark
    {
        public abstract string Name { get; }

        public virtual void SetUp()
        { }

        public virtual void TearDown()
        { }

        public virtual void Before()
        { }

        public virtual void After()
        { }

        public abstract void Run();

        public abstract int GetBytesPerRun();

        protected DocumentResource<TDocument> LoadDocumentResource<TDocument>(string resourcePath)
        {
            var bytes = File.ReadAllBytes(resourcePath);

            using (var stream = new MemoryStream(bytes))
            using (var reader = new StreamReader(stream))
            {
                var document = BsonSerializer.Deserialize<TDocument>(reader);
                return new DocumentResource<TDocument> { Bytes = bytes, Document = document };
            }
        }

        protected struct DocumentResource<TDocument>
        {
            public byte[] Bytes;
            public TDocument Document;
        }
    }
}
