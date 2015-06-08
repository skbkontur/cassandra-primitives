using System.Collections.Generic;
using System.Linq;

using BenchmarkCassandraHelpers;
using BenchmarkCassandraHelpers.Constants;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace BenchmarksCleaner
{
    public class BenchmarkCleanerExe
    {
        public static void Main(string[] args)
        {
            new BenchmarkCleanerExe().Run();
        }

        private void Run()
        {
            IProcessesCommunicator communicator = new ProcessesCommunicator(new Serializer(new AllPropertiesExtractor()));
            var actualizer = new CassandraSchemeActualizer(communicator.GetCassandraCluster(), new BenchmarkMetaProvider(new[]
            {
                new ColumnFamilyFullName(RunningProcessesConstants.Keyspace, RunningProcessesConstants.ColumnFamily),
                new ColumnFamilyFullName(StartSignalCostants.Keyspace, StartSignalCostants.ColumnFamily),
                new ColumnFamilyFullName(WriteResultsCostants.Keyspace, WriteResultsCostants.ColumnFamily),
                new ColumnFamilyFullName(ExecutingProcessesConstants.Keyspace, ExecutingProcessesConstants.ColumnFamily),
                new ColumnFamilyFullName(OldLockConstants.Keyspace, OldLockConstants.ColumnFamily),
                new ColumnFamilyFullName(NewWithCassandraTTLLockConstants.Keyspace, NewWithCassandraTTLLockConstants.ColumnFamily),
                new ColumnFamilyFullName(LockIdConstants.Keyspace, LockIdConstants.ColumnFamily),
                new ColumnFamilyFullName("CassandraPrimitives", "TimeService"),
            }), new CassandraInitializerSettings());
            actualizer.AddNewColumnFamilies();
            communicator.RemoveAllRunningProcesses();
        }
    }

    internal class BenchmarkMetaProvider : ICassandraMetadataProvider
    {
        public BenchmarkMetaProvider(ColumnFamilyFullName[] columnFamilies)
        {
            this.columnFamilies = columnFamilies;
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