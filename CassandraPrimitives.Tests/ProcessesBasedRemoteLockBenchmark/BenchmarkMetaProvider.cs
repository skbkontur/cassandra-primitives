using System.Collections.Generic;
using System.Linq;

using BenchmarkCassandraHelpers;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace ProcessesBasedRemoteLockBenchmark
{
    internal class BenchmarkMetaProvider : ICassandraMetadataProvider
    {
        public BenchmarkMetaProvider(ColumnFamilyFullName[] columnFamilies)
        {
            this.columnFamilies = columnFamilies.Concat(new[]{new ColumnFamilyFullName(RunningProcessesConstants.Keyspace, RunningProcessesConstants.ColumnFamily),}).ToArray();
        }

        public IEnumerable<Keyspace> BuildClusterKeyspaces(ICassandraInitializerSettings cassandraInitializerSettings)
        {
            var keyspaces = columnFamilies.GroupBy(x => x.KeyspaceName).Select(x => new Keyspace
            {
                Name = x.Key,
                ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy",
                ReplicationFactor = cassandraInitializerSettings.ReplicationFactor,
                ColumnFamilies = x.ToDictionary(y => y.ColumnFamilyName, y => new ColumnFamily
                {
                    Name = y.ColumnFamilyName,
                    Caching = cassandraInitializerSettings.RowCacheSize == 0 ? ColumnFamilyCaching.KeysOnly : ColumnFamilyCaching.All
                })
            }).ToArray();
            return keyspaces;
        }

        private readonly ColumnFamilyFullName[] columnFamilies;
    }
}