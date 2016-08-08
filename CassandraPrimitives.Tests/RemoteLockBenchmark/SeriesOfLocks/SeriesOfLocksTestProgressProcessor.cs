using System;
using System.Collections.Generic;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.SeriesOfLocks
{
    public class SeriesOfLocksTestProgressProcessor : AbstractTestProgressProcessor<SeriesOfLocksProgressMessage>
    {
        public SeriesOfLocksTestProgressProcessor(TestConfiguration configuration, SeriesOfLocksTestOptions testOptions, ITeamCityLogger teamCityLogger, MetricsContext metricsContext)
            : base(configuration, teamCityLogger, metricsContext)
        {
            meter = metricsContext.Meter("Soft locks", new Unit("Locks"));
            this.testOptions = testOptions;
        }

        protected override string GetTestName()
        {
            return "SeriesOfLocks";
        }

        public override string HandleProgressMessage(SeriesOfLocksProgressMessage message, int processInd)
        {
            if (message.Final)
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} finished work", processInd);
            else
                ProcessProgress(message);
            return null;
        }

        private void ProcessProgress(SeriesOfLocksProgressMessage message)
        {
            amountOfLocks += message.AmountOfLocks;
            meter.Mark(message.AmountOfLocks);
            lastAcquiredLockInd = Math.Max(lastAcquiredLockInd, message.LastAcquiredLockInd);
            ReportProgressToTeamCity();
        }

        public override string HandleLogMessage(string message, int processInd)
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} says: {1}", processInd, message);
            return null;
        }

        public override Dictionary<string, Func<object>> GetDynamicOptions()
        {
            return new Dictionary<string, Func<object>>
                {
                    {"last_acquired_lock_ind", () => lastAcquiredLockInd}
                };
        }

        protected override double GetProgressInPercents()
        {
            return amountOfLocks * 100.0 / testOptions.AmountOfLocks;
        }

        private long amountOfLocks;
        private readonly Meter meter;
        private readonly SeriesOfLocksTestOptions testOptions;
        private long lastAcquiredLockInd = -1;
    }
}