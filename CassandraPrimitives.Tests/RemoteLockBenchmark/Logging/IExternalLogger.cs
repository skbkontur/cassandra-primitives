namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Logging
{
    public interface IExternalLogger
    {
        void Log(string message);
        void Log(string format, params object[] items);
    }
}