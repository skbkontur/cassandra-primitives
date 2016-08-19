namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations
{
    public interface IEnvironmentVariableProvider
    {
        string GetValue(string name);
    }
}