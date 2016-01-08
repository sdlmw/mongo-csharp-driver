using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace Benchmarking.Benchmarks
{
    internal class RunCommandBenchmark : AbstractMongoBenchmark
    {
        private readonly int _numInternalIterations;
        private BsonDocument _command;

        public RunCommandBenchmark(int numInternalIterations)
            : base("TestRunCommand")
        {
            _numInternalIterations = numInternalIterations;
        }

        public override void SetUp()
        {
            base.SetUp();
            _command = new BsonDocument("ismaster", 1);
        }

        public override void Run()
        {
            for (int i = 0; i < _numInternalIterations; i++)
            {
                Database.RunCommand<BsonDocument>(_command);
            }
        }

        public override int GetBytesPerRun()
        {
            return _command.ToBson().Length * _numInternalIterations;
        }
    }
}
