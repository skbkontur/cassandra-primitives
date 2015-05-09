﻿using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings
{
    public class CassandraMetaProvider : ICassandraMetadataProvider
    {
        public IEnumerable<Keyspace> BuildClusterKeyspaces(ICassandraInitializerSettings cassandraInitializerSettings)
        {
            var arr = new[] {ColumnFamilies.eventLog, ColumnFamilies.eventMeta, ColumnFamilies.ticksHolder, ColumnFamilies.eventLogAdditionalInfo, ColumnFamilies.remoteLock, ColumnFamilies.newRemoteLock, ColumnFamilies.timeService, ColumnFamilies.expirationMonitoring};
            var keyspaces = arr.GroupBy(x => x.KeyspaceName).Select(x => new Keyspace
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
    }
}