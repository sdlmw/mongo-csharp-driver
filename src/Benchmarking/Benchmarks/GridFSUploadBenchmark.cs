using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmarking.Benchmarks
{
    internal class GridFSUploadBenchmark : AbstractGridFSBenchmark
    {
        public GridFSUploadBenchmark(string resourcePath)
            : base("TestGridFsUpload", resourcePath)
        { }

        public override void SetUp()
        {
            base.SetUp();
            _bucket.UploadFromBytes("gridfstest", _bytes);
        }

        public override void Before()
        {
            base.Before();
            DropDatabase();
            _bucket.UploadFromBytes("small", new byte[1]);
        }

        public override void Run()
        {
            _bucket.UploadFromBytes("gridfstest", _bytes);
        }
    }
}
