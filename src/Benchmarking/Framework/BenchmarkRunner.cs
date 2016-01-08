using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Benchmarking.Framework
{
    internal class BenchmarkRunner
    {
        public BenchmarkRunner(Benchmark benchmark, int numWarmupIterations, int numIterations, TimeSpan minTime, TimeSpan maxTime)
        {
            Benchmark = benchmark;
            NumWarmupIterations = numWarmupIterations;
            NumIterations = numIterations;
            MinTime = minTime;
            MaxTime = maxTime;
        }

        public Benchmark Benchmark { get; }

        public TimeSpan MaxTime { get; }

        public TimeSpan MinTime { get; }

        public int NumWarmupIterations { get; }

        public int NumIterations { get; }

        public BenchmarkResult Run()
        {
            Benchmark.SetUp();

            for (int i = 0; i < NumWarmupIterations; i++)
            {
                Benchmark.Before();
                Benchmark.Run();
                Benchmark.After();
            }

            var results = new List<TimeSpan>();

            var elapsed = TimeSpan.Zero;
            var sw = new Stopwatch();
            for (int i = 0; !IsFinished(i, elapsed); i++)
            {
                Benchmark.Before();

                sw.Restart();
                Benchmark.Run();
                sw.Stop();

                results.Add(sw.Elapsed);
                elapsed += sw.Elapsed;

                Benchmark.After();
            }

            Benchmark.TearDown();

            return new BenchmarkResult(Benchmark.Name, results, Benchmark.GetBytesPerRun());
        }

        private bool IsFinished(int iteration, TimeSpan elapsed)
        {
            if (elapsed <= MinTime)
            {
                return false;
            }

            if (elapsed >= MaxTime)
            {
                return true;
            }

            return iteration >= NumIterations;
        }
    }
}
