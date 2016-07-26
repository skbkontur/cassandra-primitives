namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging
{
    public interface IExternalLogger
    {
        void Log(string message);
        void Log(string format, params object[] items);
    }
}