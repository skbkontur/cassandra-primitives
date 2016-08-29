using System;
using System.Linq;

using Cassandra;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.CasRemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.CASCassandra
{
    public class CassandraCasRemoteLockGetter : IRemoteLockGetter
    {
        private readonly CasRemoteLocker locker;

        public CassandraCasRemoteLockGetter(ICassandraClusterSettings cassandraClusterSettings)
        {
            var lockProvider = new CasRemoteLockProvider(cassandraClusterSettings.Endpoints.ToList(),
                "RemoteLockBenchmark",
                "CASRemoteLock",
                ConsistencyLevel.Quorum,
                TimeSpan.FromMinutes(5),
                TimeSpan.FromSeconds(30));

            lockProvider.ActualiseTables();
            lockProvider.InitPreparedStatements();
            locker = lockProvider.CreateLocker();
        }

        public IRemoteLock Get(string lockId)
        {
            return new CassandraCasRemoteLock(locker, lockId);
        }
    }
}