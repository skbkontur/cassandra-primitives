using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging
{
    public interface IExternalProgressLogger<in TProgressMessage> : IExternalLogger
        where TProgressMessage : IProgressMessage
    {
        void PublishProgress(TProgressMessage progressMessage);
    }
}