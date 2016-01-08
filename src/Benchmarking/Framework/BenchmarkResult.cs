using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmarking.Framework
{
    internal class BenchmarkResult
    {
        public BenchmarkResult(string name, IReadOnlyList<TimeSpan> results, int bytesPerRun)
        {
            Name = name;
            Results = results.OrderBy(x => x).ToList();
            BytesPerRun = bytesPerRun;
        }

        public string Name { get; }

        public IReadOnlyList<TimeSpan> Results { get; }

        public int BytesPerRun { get; }

        public TimeSpan GetResultAtPercentile(int percentile)
        {
            int midIndex = (Results.Count * percentile / 100) - 1;
            return Results[Math.Max(0, midIndex)];
        }
    }
}
