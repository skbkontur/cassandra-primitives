using System.IO;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning
{
    public class RemoteDirectory
    {
        public RemoteDirectory(string remotePrefix, string localPrefix, string relativePath)
        {
            this.remotePrefix = remotePrefix;
            this.localPrefix = localPrefix;
            this.relativePath = relativePath;
        }

        public string AsLocal { get { return Path.Combine(localPrefix, relativePath); } }
        public string AsRemote { get { return Path.Combine(remotePrefix, relativePath); } }

        public RemoteDirectory Combine(params string[] path)
        {
            return new RemoteDirectory(remotePrefix, localPrefix, Path.Combine(relativePath, Path.Combine(path)));
        }

        private readonly string remotePrefix;
        private readonly string localPrefix;
        private readonly string relativePath;
    }
}