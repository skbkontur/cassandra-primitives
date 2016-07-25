using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ProgressMessages;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ExternalLogging
{
    public interface IExternalProgressLogger<in TProgressMessage> : IExternalLogger
        where TProgressMessage : IProgressMessage
    {
        void PublishProgress(TProgressMessage progressMessage);
    }
}