namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging
{
    public interface IExternalLogger
    {
        void Log(string message);
        void Log(string format, params object[] items);
    }
}