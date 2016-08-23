using System;
using System.Linq;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.CasRemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.CASCassandra
{
    public class CassandraCasRemoteLockGetter : IRemoteLockGetter
    {
        private readonly CasRemoteLocker casRemoteLocker;

        public CassandraCasRemoteLockGetter(ICassandraClusterSettings cassandraClusterSettings)
        {
            casRemoteLocker = new CasRemoteLocker(
                cassandraClusterSettings
                    .Endpoints
                    .Select(endpoint => new IPEndPoint(endpoint.Address, 9343))//TODO port
                    .ToList(),
                "RemoteLockBenchmark",
                "CASRemoteLock",
                TimeSpan.FromMinutes(5));
            casRemoteLocker.ActualiseTables();
        }

        public IRemoteLock Get(string lockId)
        {
            return new CassandraCasRemoteLock(casRemoteLocker, lockId);
        }
    }
}