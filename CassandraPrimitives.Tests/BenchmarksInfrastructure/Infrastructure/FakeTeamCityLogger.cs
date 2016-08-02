using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure
{
    internal class FakeTeamCityLogger : ITeamCityLogger
    {
        public void BeginMessageBlock(string blockName)
        {
        }

        public void EndMessageBlock()
        {
        }

        public void ReportActivity(string activityName)
        {
        }

        public void BeginActivity(string activityName)
        {
        }

        public void EndActivity()
        {
        }

        public void WriteMessage(TeamCityMessageSeverity severity, string text, string errorDetails = null)
        {
        }

        public void WriteMessageFormat(TeamCityMessageSeverity severity, string text, params object[] parameters)
        {
        }

        public void SetBuildStatus(string buildStatus, string buildStatusText)
        {
        }

        public void PublishArtifact(string path)
        {
        }

        public void ReportStatisticsValue(string name, string value)
        {
        }
    }
}