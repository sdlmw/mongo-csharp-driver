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
    internal class GridFSExportBenchmark : AbstractMongoBenchmark
    {
        private readonly string _exportDir;
        private readonly IEnumerable<string> _resourcePaths;
        private GridFSBucket _bucket;

        public GridFSExportBenchmark(string exportDir, IEnumerable<string> resourcePaths)
            : base("TestGridFsMultiExport")
        {
            _exportDir = Path.Combine(exportDir, "export");
            _resourcePaths = resourcePaths;
        }

        public override void SetUp()
        {
            base.SetUp();
            DropDatabase();
            _bucket = new GridFSBucket(Database);

            Parallel.ForEach(_resourcePaths, resourcePath =>
            {
                var filename = Path.GetFileName(resourcePath);
                using (var stream = File.OpenRead(resourcePath))
                {
                    _bucket.UploadFromStream(filename, stream);
                }
            });
        }

        public override void Before()
        {
            base.Before();
            Directory.CreateDirectory(_exportDir);
        }

        public override void Run()
        {
            Parallel.ForEach(_resourcePaths, resourcePath =>
            {
                var filename = Path.GetFileName(resourcePath);
                using (var stream = File.OpenWrite(Path.Combine(_exportDir, filename)))
                {
                    _bucket.DownloadToStreamByName(filename, stream);
                }
            });
        }

        public override void After()
        {
            base.After();
            foreach (var file in Directory.GetFiles(_exportDir))
            {
                File.Delete(file);
            }
            Directory.Delete(_exportDir);
        }

        public override int GetBytesPerRun()
        {
            return 262144000;
        }
    }
}
