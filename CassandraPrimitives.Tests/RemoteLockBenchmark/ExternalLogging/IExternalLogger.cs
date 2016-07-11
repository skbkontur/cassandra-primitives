namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging
{
    public interface IExternalLogger
    {
        void Log(string message);
        void Log(string format, params object[] items);
    }
}