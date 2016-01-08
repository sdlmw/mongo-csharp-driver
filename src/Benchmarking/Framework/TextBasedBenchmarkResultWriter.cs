using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmarking.Framework
{
    internal class TextBasedBenchmarkResultWriter : IBenchmarkResultWriter
    {
        private static readonly int[] _percentiles = new[] { 0, 10, 25, 50, 75, 90, 100 };
        private readonly TextWriter _writer;

        public TextBasedBenchmarkResultWriter(TextWriter writer)
        {
            _writer = writer;
        }

        public void Write(BenchmarkResult result)
        {
            _writer.WriteLine(result.Name);
            _writer.WriteLine(new string('-', result.Name.Length));

            var megaBytesPerRun = result.BytesPerRun / 1000000;
            var meanSeconds = result.Results.Average(x => x.TotalSeconds);
            var varianceSeconds = result.Results.Select(x => Math.Pow(x.TotalSeconds - meanSeconds, 2)).Sum() / result.Results.Count;
            var stdDevSeconds = Math.Sqrt(varianceSeconds);

            _writer.WriteLine(result.Results.Count + " runs");
            _writer.WriteLine("  avg: {0:F6} sec/run", meanSeconds);
            _writer.WriteLine("    σ: {0:F6} sec/run", stdDevSeconds);

            foreach (var percentile in _percentiles)
            {
                var secondsPerRun = result.GetResultAtPercentile(percentile).TotalSeconds;
                _writer.WriteLine(@"{0, 4}%: {1:F6} sec/run, {2:F6} MB/sec", percentile, secondsPerRun, megaBytesPerRun / secondsPerRun);
            }

            _writer.WriteLine();
        }

    }
}
