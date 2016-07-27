using System.Collections.Generic;

using Metrics;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors
{
    public class WaitForLockTestProgressProcessor : AbstractTestProgressProcessor
    {
        public WaitForLockTestProgressProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger)
            : base(configuration, teamCityLogger)
        {
            histogram = Metric.Histogram("Time waiting for lock", new Unit("ms"));
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

        public override string HandlePublishProgress(string request, int processInd)
        {
            var progressMessage = JsonConvert.DeserializeObject<WaitForLockProgressMessage>(request);

            if (progressMessage.Final)
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} finished work", processInd);
            else
                ProcessLockEvents(progressMessage.LockWaitingDurationsMs);
            return null;
        }

        public override string HandleLog(string request, int processInd)
        {
            var log = JObject.Parse(request);
            var message = log["message"].ToString();

            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} says: {1}", processInd, message);
            return null;
        }

        private int totalLocksAcquired;
        private readonly Histogram histogram;
    }
}