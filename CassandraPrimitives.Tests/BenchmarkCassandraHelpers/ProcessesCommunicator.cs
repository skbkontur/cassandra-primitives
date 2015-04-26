using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;

using GroBuf;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.ClusterDeployment;

namespace BenchmarkCassandraHelpers
{
    public class ProcessesCommunicator : IProcessesCommunicator
    {
        public ProcessesCommunicator(Serializer serializer)
        {
            this.serializer = serializer;
        }

        public void AddRunningProcess(string processId)
        {
            MakeInConnection(
                RunningProcessesConstants.Keyspace,
                RunningProcessesConstants.ColumnFamily,
                connection => connection.AddColumn(RunningProcessesConstants.Row, new Column
                {
                    Name = processId,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Value = new byte[0],
                })
                );
        }

        public void RemoveRunningProcess(string processId)
        {
            MakeInConnection(
                RunningProcessesConstants.Keyspace,
                RunningProcessesConstants.ColumnFamily,
                connection => connection.DeleteColumn(RunningProcessesConstants.Row, processId)
                );
        }

        public void WaitStartSignal(string keyspace, string columnFamily)
        {
            while(true)
            {
                var success = false;
                MakeInConnection(
                    keyspace,
                    columnFamily,
                    connection => { success = connection.GetRow(StartSignalCostants.Row).Any(); });
                if(success)
                    break;
                Console.WriteLine("Failed to get start signal in {0} {1}", keyspace, columnFamily);
                Thread.Sleep(50);
            }
        }

        public void SendStartSignal(string keyspace, string columnFamily)
        {
            Console.WriteLine("SIGNAL {0} {1}", keyspace, columnFamily);
            MakeInConnection(
                keyspace,
                columnFamily,
                connection => connection.AddColumn(StartSignalCostants.Row, new Column
                {
                    Name = StartSignalCostants.Column,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Value = new byte[0],
                })
                );
        }

        public void WriteResults(string keyspace, string columnFamily, string processId, double[] times)
        {
            MakeInConnection(keyspace, columnFamily, connection => connection.AddBatch(processId, times.Select((value, index) => new Column
            {
                Name = index.ToString("000000000"),
                Timestamp = DateTime.UtcNow.Ticks,
                Value = serializer.Serialize(value),
            })));
        }

        public double[] GetResults(string keyspace, string columnFamily, string processId)
        {
            double[] result = {};
            MakeInConnection(keyspace, columnFamily, connection => { result = connection.GetRow(processId).Select(x => serializer.Deserialize<double>(x.Value)).ToArray(); });
            return result;
        }

        public ICassandraCluster GetCassandraCluster()
        {
            return new CassandraCluster(GetCassandraNode().CreateSettings(IPAddress.Loopback));
        }

        public int GetRunningProcessesCount()
        {
            var result = 0;
            MakeInConnection(
                RunningProcessesConstants.Keyspace,
                RunningProcessesConstants.ColumnFamily,
                connection => { result = connection.GetCount(RunningProcessesConstants.Row); });
            return result;
        }

        public CassandraNode GetCassandraNode()
        {
            return new CassandraNode(Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"1.2"))
            {
                Name = "node_at_9360",
                JmxPort = 7399,
                GossipPort = 7400,
                RpcPort = 9360,
                CqlPort = 9343,
                DataBaseDirectory = @"../data/",
                DeployDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\Cassandra1.2"),
                ListenAddress = "127.0.0.1",
                RpsAddress = "0.0.0.0",
                SeedAddresses = new[] {"127.0.0.1"},
                InitialToken = "",
                ClusterName = "test_cluster"
            };
        }

        private void MakeInConnection(string keyspace, string columnFamily, Action<IColumnFamilyConnection> action)
        {
            action(GetCassandraCluster().RetrieveColumnFamilyConnection(keyspace, columnFamily));
        }

        private string FindCassandraTemplateDirectory(string currentDir)
        {
            if(currentDir == null)
                throw new Exception("Невозможно найти каталог с Cassandra-шаблонами");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private readonly Serializer serializer;

        private const string cassandraTemplates = @"Assemblies\CassandraTemplates";
    }
}