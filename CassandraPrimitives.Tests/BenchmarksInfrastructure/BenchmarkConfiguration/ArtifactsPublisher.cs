using System;
using System.IO;
using System.IO.Compression;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration
{
    public class ArtifactsPublisher : IDisposable
    {
        public ArtifactsPublisher(ITeamCityLogger teamCityLogger, string currentArtifactsDir, string targetDir, string targetName, CompressionLevel compressionLevel = CompressionLevel.Optimal)
        {
            this.currentArtifactsDir = new DirectoryInfo(currentArtifactsDir);
            this.targetDir = new DirectoryInfo(targetDir);
            this.teamCityLogger = teamCityLogger;
            this.targetName = targetName;
            this.compressionLevel = compressionLevel;
            if (this.currentArtifactsDir.Exists)
                this.currentArtifactsDir.Delete(true);
        }

        public void Dispose()
        {
            using(teamCityLogger.MessageBlock("Compressing artifacts"))
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Going to compress and publish artifacts");
                for (int i = 0; i < 5 && !currentArtifactsDir.Exists; i++)
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Current artifacts directory ({0}) does not exist, waiting for it...", currentArtifactsDir);
                    Thread.Sleep(5000);
                }
                if (currentArtifactsDir.Exists)
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Current artifacts directory ({0}) exists, going to create zip", currentArtifactsDir);
                    var testArtifactsPath = Path.Combine(targetDir.FullName, string.Format("{0}.zip", targetName));
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Will create zip with full name {0}", testArtifactsPath);
                    ZipFile.CreateFromDirectory(currentArtifactsDir.FullName, testArtifactsPath, compressionLevel, false);
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Zip was created");
                }
                else
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Current artifacts directory ({0}) does not exist, artifacts weren't published", currentArtifactsDir);
                }
            }
        }

        private readonly DirectoryInfo currentArtifactsDir, targetDir;
        private readonly ITeamCityLogger teamCityLogger;
        private readonly string targetName;
        private readonly CompressionLevel compressionLevel;
    }
}