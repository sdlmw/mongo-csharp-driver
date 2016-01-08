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
    internal class GridFSImportBenchmark : AbstractMongoBenchmark
    {
        private readonly IEnumerable<string> _resourcePaths;
        private GridFSBucket _bucket;

        public GridFSImportBenchmark(IEnumerable<string> resourcePaths)
            : base("TestGridFsMultiImport")
        {
            _resourcePaths = resourcePaths.ToList();
        }

        public override void SetUp()
        {
            base.SetUp();
            _bucket = new GridFSBucket(Database);
        }

        public override void Before()
        {
            base.Before();
            DropDatabase();
            _bucket.UploadFromBytes("small", new byte[1]);
        }

        public override void Run()
        {
            Parallel.ForEach(_resourcePaths, resourcePath =>
            {
                var filename = Path.GetFileName(resourcePath);
                using (var stream = File.OpenRead(resourcePath))
                {
                    _bucket.UploadFromStream(filename, stream);
                }
            });
        }

        public override int GetBytesPerRun()
        {
            return 262144000;
        }
    }
}
