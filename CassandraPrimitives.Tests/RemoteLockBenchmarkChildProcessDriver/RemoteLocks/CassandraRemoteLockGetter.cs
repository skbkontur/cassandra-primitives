﻿using System;
using System.Collections.Generic;

using GroBuf;
using GroBuf.DataMembersExtracters;

using log4net;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.ExternalLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.CassandraSettings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.ExternalLogging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.RemoteLocks
{
    public class CassandraRemoteLockGetter : IRemoteLockGetter, IDisposable
    {
        public CassandraRemoteLockGetter(ICassandraClusterSettings cassandraClusterSettings, IExternalLogger externalLogger)
        {
            remoteLockersToDispose = new List<RemoteLocker>();
            logger = LogManager.GetLogger(GetType());
            this.externalLogger = externalLogger;

            cassandraCluster = new CassandraCluster(cassandraClusterSettings);
        }

        public IRemoteLockCreator[] Get(int amount)
        {
            var serializer = new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead);
            var implementationSettings = CassandraRemoteLockImplementationSettings.Default(ColumnFamilies.remoteLock);

            var remoteLockerMetrics = new RemoteLockerMetrics("dummyKeyspace");

            var remoteLockers = new IRemoteLockCreator[amount];
            for (var i = 0; i < amount; i++)
            {
                var cassandraRemoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, implementationSettings);
                var remoteLocker = new RemoteLocker(cassandraRemoteLockImplementation, remoteLockerMetrics);
                remoteLockers[i] = remoteLocker;
                remoteLockersToDispose.Add(remoteLocker);
            }

            return remoteLockers;
        }

        public void Dispose()
        {
            foreach (var remoteLocker in remoteLockersToDispose)
            {
                try
                {
                    remoteLocker.Dispose();
                }
                catch (Exception e)
                {
                    logger.Error("Exception occured while disposing remoteLocker:", e);
                    externalLogger.Log("Exception occured while disposing remoteLocker:\n{0}", e);
                }
            }
            cassandraCluster.Dispose();
        }

        private readonly List<RemoteLocker> remoteLockersToDispose;
        private readonly ILog logger;
        private readonly IExternalLogger externalLogger;
        private readonly ICassandraCluster cassandraCluster;
    }
}