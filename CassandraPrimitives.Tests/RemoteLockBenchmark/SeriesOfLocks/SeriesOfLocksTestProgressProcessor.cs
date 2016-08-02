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
        }

        public override string HandleLogMessage(string message, int processInd)
        {
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} says: {1}", processInd, message);
            return null;
        }

        protected override double GetProgressInPercents()
        {
            return amountOfLocks * 100.0 / (configuration.AmountOfProcesses * configuration.AmountOfThreads * testOptions.AmountOfLocks);
        }

        private long amountOfLocks;
        private readonly Meter meter;
        private readonly SeriesOfLocksTestOptions testOptions;
    }
}