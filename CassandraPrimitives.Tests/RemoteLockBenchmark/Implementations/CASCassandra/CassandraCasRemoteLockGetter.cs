using System;
using System.Linq;
using System.Net;

using Cassandra;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.CasRemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.CASCassandra
{
    public class CassandraCasRemoteLockGetter : IRemoteLockGetter
    {
        private readonly CasRemoteLockProvider lockProvider;

        public CassandraCasRemoteLockGetter(ICassandraClusterSettings cassandraClusterSettings)
        {
            /*lockProvider = new CasRemoteLockProvider(
                cassandraClusterSettings
                    .Endpoints
                    .Select(endpoint => new IPEndPoint(endpoint.Address, 9343))//TODO port
                    .ToList(),
                "RemoteLockBenchmark",
                "CASRemoteLock",
                ConsistencyLevel.Quorum);*/

            lockProvider = new CasRemoteLockProvider(new[]
                    {
                        new IPEndPoint(IPAddress.Parse("10.217.9.2"), 9042),
                        new IPEndPoint(IPAddress.Parse("10.217.8.243"), 9042),
                        new IPEndPoint(IPAddress.Parse("10.217.8.242"), 9042)
                    }.ToList(),
                "RemoteLockBenchmark",
                "TestCASRemoteLock",
                ConsistencyLevel.Quorum);

            lockProvider.ActualiseTables();
        }

        public IRemoteLock Get(string lockId)
        {
            return new CassandraCasRemoteLock(lockProvider.CreateLocker(TimeSpan.FromMinutes(5)), lockId);
        }
    }
}