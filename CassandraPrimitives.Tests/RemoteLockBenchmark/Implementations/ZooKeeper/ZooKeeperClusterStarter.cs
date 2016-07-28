using System;
using System.Collections.Generic;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.ZookeeperInitialisaton;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.ZooKeeper.ZookeeperSettings;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.ZooKeeper
{
    public class ZookeeperClusterStarter : IDisposable
    {
        public ZookeeperClusterStarter(ZookeeperClusterSettings clusterSettings, List<ZookeeperRemoteNodeStartInfo> remoteNodeStartInfos, ITeamCityLogger teamCityLogger)
        {
            zookeeperInitialisers = new List<RemoteZookeeperInitializer>();
            ClusterSettings = clusterSettings;
            this.teamCityLogger = teamCityLogger;
            try
            {
                foreach (var remoteNodeStartInfo in remoteNodeStartInfos)
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Initialising zookeeper on {0}...", remoteNodeStartInfo.Credentials.MachineName);
                    var zookeeperInitializer = new RemoteZookeeperInitializer(remoteNodeStartInfo.Credentials, remoteNodeStartInfo.RemoteWorkDir, remoteNodeStartInfo.TaskWrapperPath);
                    zookeeperInitialisers.Add(zookeeperInitializer);
                    zookeeperInitializer.CreateAndStartNode(remoteNodeStartInfo.Settings);
                }
            }
            catch (Exception)
            {
                DisposeZookeeperInitialisers();
                throw;
            }
        }

        public ZookeeperClusterSettings ClusterSettings { get; private set; }

        private void DisposeZookeeperInitialisers()
        {
            if (zookeeperInitialisers == null)
                return;
            foreach (var zookeeperInitializer in zookeeperInitialisers)
                zookeeperInitializer.Dispose();
        }

        public void Dispose()
        {
            DisposeZookeeperInitialisers();
        }

        private readonly List<RemoteZookeeperInitializer> zookeeperInitialisers;
        private readonly ITeamCityLogger teamCityLogger;
    }
}