using Metrics;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors
{
    public class SeriesOfLocksTestProgressProcessor : AbstractTestProgressProcessor<SeriesOfLocksProgressMessage>
    {
        public SeriesOfLocksTestProgressProcessor(TestConfiguration configuration, ITeamCityLogger teamCityLogger, MetricsContext metricsContext)
            : base(configuration, teamCityLogger, metricsContext)
        {
            meter = metricsContext.Meter("Locks processes", new Unit("Locks"));
        }

        protected override string GetTestName()
        {
            return "SeriesOfLocks";
        }

        public override string HandlePublishProgress(SeriesOfLocksProgressMessage message, int processInd)
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
            return amountOfLocks * 100.0 / (configuration.AmountOfProcesses * configuration.AmountOfThreads * configuration.AmountOfLocksPerThread);
        }

        private long amountOfLocks;
        private readonly Meter meter;
    }
}