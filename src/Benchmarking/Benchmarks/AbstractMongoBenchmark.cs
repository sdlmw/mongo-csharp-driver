using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Benchmarking.Framework;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Benchmarking.Benchmarks
{
    internal abstract class AbstractMongoBenchmark : Benchmark
    {
        private const string DBNAME = "perftest";
        private const string COLNAME = "corpus";

        private readonly IMongoClient _client;
        private readonly IMongoDatabase _database;

        protected AbstractMongoBenchmark(string name)
        {
            Name = name;
            _client = new MongoClient();
            _database = _client.GetDatabase(DBNAME);
        }

        public override string Name { get; }

        protected IMongoClient Client => _client;
        protected IMongoDatabase Database => _database;
        protected IMongoCollection<TDocument> GetCollection<TDocument>()
        {
            return _database.GetCollection<TDocument>(COLNAME);
        }

        protected void CreateCollection()
        {
            Database.CreateCollection(COLNAME, new CreateCollectionOptions<BsonDocument>());
        }

        protected void DropCollection()
        {
            Database.DropCollection(COLNAME);
        }

        protected void DropDatabase()
        {
            Client.DropDatabase(_database.DatabaseNamespace.DatabaseName);
        }
    }
}
