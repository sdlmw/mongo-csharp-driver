using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmarking.Framework
{
    internal interface IBenchmarkResultWriter
    {
        void Write(BenchmarkResult result);
    }
}
