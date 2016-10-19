namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations
{
    public class TeamCityBuildInfo
    {
        public TeamCityBuildInfo(IEnvironmentVariableProvider variableProvider)
        {
            Branch = variableProvider.GetValue("Branch");
            CommitHash = variableProvider.GetValue("CommitHash");
            BuildNumber = variableProvider.GetValue("BuildNumber");
            BuildId = variableProvider.GetValue("BuildId");
            MainAgent = variableProvider.GetValue("MainAgent");
        }

        public string Branch { get; private set; }
        public string CommitHash { get; private set; }
        public string BuildNumber { get; private set; }
        public string BuildId { get; private set; }
        public string MainAgent { get; private set; }

        public override string ToString()
        {
            return string.Format("Branch = {0}\n" +
                   "CommitHash = {1}\n" +
                   "BuildNumber = {2}\n" +
                   "BuildId = {3}\n" +
                   "MainAgent = {4}",
                   Branch,
                   CommitHash,
                   BuildNumber,
                   BuildId,
                   MainAgent);
        }
    }
}