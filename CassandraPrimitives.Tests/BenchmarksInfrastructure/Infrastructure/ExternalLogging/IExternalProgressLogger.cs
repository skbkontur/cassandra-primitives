using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging
{
    public interface IExternalProgressLogger : IExternalLogger
    {
        void PublishProgress<TProgressMessage>(TProgressMessage progressMessage)
            where TProgressMessage : IProgressMessage;
    }
}