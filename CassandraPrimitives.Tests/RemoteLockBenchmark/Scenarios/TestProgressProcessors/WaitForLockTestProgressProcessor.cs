using System.Collections.Generic;

using Metrics;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors
{
    public class WaitForLockTestProgressProcessor : AbstractTestProgressProcessor<WaitForLockProgressMessage>
    {
        public WaitForLockTestProgressProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger, MetricsContext metricsContext)
            : base(configuration, teamCityLogger, metricsContext)
        {
            histogram = metricsContext.Histogram("Time waiting for lock", new Unit("ms"));
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
            var totalAmountOfLocks = configuration.AmountOfProcesses * configuration.AmountOfThreads * configuration.AmountOfLocksPerThread;
            return totalLocksAcquired * 100.0 / totalAmountOfLocks;
        }

        protected override string GetTestName()
        {
            return "WaitForLockTest";
        }

        public override string HandlePublishProgress(WaitForLockProgressMessage message, int processInd)
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
    }
}