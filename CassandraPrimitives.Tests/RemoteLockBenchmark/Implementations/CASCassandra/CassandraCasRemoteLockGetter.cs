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
        private readonly CasRemoteLocker locker;

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

            var lockProvider = new CasRemoteLockProvider(
                new[]
                    {
                        new IPEndPoint(IPAddress.Parse("10.33.63.133"), 9042),
                        new IPEndPoint(IPAddress.Parse("10.33.61.141"), 9042),
                        new IPEndPoint(IPAddress.Parse("10.33.62.133"), 9042)
                    }.ToList(),
                "RemoteLockBenchmark",
                "TestCASRemoteLock",
                ConsistencyLevel.Quorum,
                TimeSpan.FromMinutes(5));

            lockProvider.ActualiseTables();
            locker = lockProvider.CreateLocker();
        }

        public IRemoteLock Get(string lockId)
        {
            return new CassandraCasRemoteLock(locker, lockId);
        }
    }
}