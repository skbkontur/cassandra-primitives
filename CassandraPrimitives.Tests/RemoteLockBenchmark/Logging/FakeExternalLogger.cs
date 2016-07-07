namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Logging
{
    public class FakeExternalLogger : IExternalLogger
    {
        public void Log(string format, params object[] items)
        {
        }

        public void Log(string message)
        {
        }
    }
}