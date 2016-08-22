using System.Collections.Generic;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.WaitForLock
{
    public class WaitForLockTestProgressProcessor : AbstractTestProgressProcessor<WaitForLockProgressMessage>
    {
        public WaitForLockTestProgressProcessor(TestConfiguration configuration, WaitForLockTestOptions testOptions, ITeamCityLogger teamCityLogger, MetricsContext metricsContext)
            : base(configuration, teamCityLogger, metricsContext)
        {
            histogram = metricsContext.Histogram("Time waiting for lock", new Unit("ms"));
            this.testOptions = testOptions;
        }

        private void ProcessLockEvents(List<long> lockWaitingTimes)
        {
            if (lockWaitingTimes == null || lockWaitingTimes.Count == 0)
                return;

            totalLocksAcquired += lockWaitingTimes.Count;

            foreach (var lockWaitingTime in lockWaitingTimes)
                histogram.Update(lockWaitingTime);

            ReportProgressToTeamCity();
        }

        protected override double GetProgressInPercents()
        {
            var totalAmountOfLocks = configuration.AmountOfProcesses * configuration.AmountOfThreads * testOptions.AmountOfLocks;
            return totalLocksAcquired * 100.0 / totalAmountOfLocks;
        }

        protected override string GetTestName()
        {
            return "WaitForLockTest";
        }

        public override string HandleProgressMessage(WaitForLockProgressMessage message, int processInd)
        {
            if (message.Final)
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} finished work", processInd);
            else
                ProcessLockEvents(message.LockWaitingDurationsMs);
            return null;
        }

        public override string HandleLogMessage(string message, int processInd)
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} says: {1}", processInd, message);
            return null;
        }

        private int totalLocksAcquired;
        private readonly Histogram histogram;
        private readonly WaitForLockTestOptions testOptions;
    }
}