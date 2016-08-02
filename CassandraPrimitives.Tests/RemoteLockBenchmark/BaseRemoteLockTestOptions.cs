using System.Collections.Generic;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class BaseRemoteLockTestOptions : ITestOptions
    {
        public BaseRemoteLockTestOptions(int amountOfLocksPerThread, int minWaitTimeMilliseconds, int maxWaitTimeMilliseconds)
        {
            AmountOfLocksPerThread = amountOfLocksPerThread;
            MinWaitTimeMilliseconds = minWaitTimeMilliseconds;
            MaxWaitTimeMilliseconds = maxWaitTimeMilliseconds;
        }

        public int AmountOfLocksPerThread { get; private set; }
        public int MinWaitTimeMilliseconds { get; private set; }
        public int MaxWaitTimeMilliseconds { get; private set; }

        public static List<BaseRemoteLockTestOptions> ParseWithRanges(IRemoteLockBenchmarkEnvironment environment)
        {
            var amountOfThreads = OptionsParser.ParseInts("AmountOfLocksPerThread", environment.AmountOfLocksPerThread);
            var amountOfProcesses = OptionsParser.ParseInts("MinWaitTimeMilliseconds", environment.MinWaitTimeMilliseconds);
            var amountOfClusterNodes = OptionsParser.ParseInts("MaxWaitTimeMilliseconds", environment.MaxWaitTimeMilliseconds);

            var combinations = OptionsParser.Product(
                amountOfThreads.Cast<object>().ToList(),
                amountOfProcesses.Cast<object>().ToList(),
                amountOfClusterNodes.Cast<object>().ToList());

            return combinations
                .Select(combination => new BaseRemoteLockTestOptions(
                                           (int)combination[0],
                                           (int)combination[1],
                                           (int)combination[2]))
                .ToList();
        }
    }
}