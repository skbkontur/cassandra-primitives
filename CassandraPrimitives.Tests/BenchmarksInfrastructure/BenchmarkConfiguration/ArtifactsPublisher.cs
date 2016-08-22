using System;
using System.IO;
using System.IO.Compression;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration
{
    public class ArtifactsPublisher : IDisposable
    {
        public ArtifactsPublisher(string currentArtifactsDir, string targetDir, string targetName, CompressionLevel compressionLevel = CompressionLevel.Optimal)
        {
            this.currentArtifactsDir = new DirectoryInfo(currentArtifactsDir);
            this.targetDir = new DirectoryInfo(targetDir);
            this.targetName = targetName;
            this.compressionLevel = compressionLevel;
            if (this.currentArtifactsDir.Exists)
                this.currentArtifactsDir.Delete(true);
        }

        public void Dispose()
        {
            if (currentArtifactsDir.Exists)
            {
                var testArtifactsPath = Path.Combine(targetDir.FullName, string.Format("{0}.zip", targetName));
                ZipFile.CreateFromDirectory(currentArtifactsDir.FullName, testArtifactsPath, compressionLevel, false);
            }
        }

        private readonly DirectoryInfo currentArtifactsDir, targetDir;
        private readonly string targetName;
        private readonly CompressionLevel compressionLevel;
    }
}