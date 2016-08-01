namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging
{
    public interface IExternalLogger
    {
        void Log(string message);
        void Log(string format, params object[] items);
    }
}