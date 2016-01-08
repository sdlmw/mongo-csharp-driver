using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;

namespace Benchmarking.Benchmarks
{
    internal class GridFSDownloadBenchmark : AbstractGridFSBenchmark
    {
        private ObjectId _fileId;

        public GridFSDownloadBenchmark(string resourcePath)
            : base("TestGridFsDownload", resourcePath)
        { }

        public override void SetUp()
        {
            base.SetUp();
            _fileId = _bucket.UploadFromBytes("gridfstest", _bytes);
        }

        public override void Run()
        {
            _bucket.DownloadAsBytes(_fileId);
        }
    }
}
