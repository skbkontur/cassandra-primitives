using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging
{
    public interface IExternalProgressLogger : IExternalLogger
    {
        void PublishProgress<TProgressMessage>(TProgressMessage progressMessage)
            where TProgressMessage : IProgressMessage;
    }
}