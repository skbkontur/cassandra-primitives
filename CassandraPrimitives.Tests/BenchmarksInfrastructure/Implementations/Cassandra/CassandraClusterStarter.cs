﻿using System;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.Cassandra.CassandraSettings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.Cassandra
{
    public class CassandraClusterStarter : IDisposable
    {
        public CassandraClusterStarter(CassandraClusterSettings clusterSettings, List<CassandraRemoteNodeStartInfo> remoteNodeStartInfos)
        {
            cassandraInitialisers = new List<ICassandraInitialiser>();
            ClusterSettings = clusterSettings;
            try
            {
                foreach (var remoteNodeStartInfo in remoteNodeStartInfos)
                {
                    var cassandraInitializer = new RemoteCassandraInitializer(remoteNodeStartInfo.Credentials, remoteNodeStartInfo.RemoteWorkDir, remoteNodeStartInfo.TaskWrapperRelativePath);
                    cassandraInitialisers.Add(cassandraInitializer);
                    cassandraInitializer.CreateNode(remoteNodeStartInfo.Settings);
                }
                using (var cassandraCluster = new CassandraCluster(ClusterSettings))
                {
                    var initializerSettings = new CassandraInitializerSettings();
                    var cassandraSchemeActualizer = new CassandraSchemeActualizer(cassandraCluster, new CassandraMetaProvider(), initializerSettings);
                    cassandraSchemeActualizer.AddNewColumnFamilies();
                }
            }
            catch (Exception)
            {
                DisposeCassandraInitialisers();
                throw;
            }
        }

        public ICassandraClusterSettings ClusterSettings { get; private set; }

        private void DisposeCassandraInitialisers()
        {
            if (cassandraInitialisers == null)
                return;
            foreach (var cassandraInitializer in cassandraInitialisers)
                cassandraInitializer.Dispose();
        }

        public void Dispose()
        {
            DisposeCassandraInitialisers();
        }

        private readonly List<ICassandraInitialiser> cassandraInitialisers;
    }
}