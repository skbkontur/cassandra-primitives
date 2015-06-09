using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;

using BenchmarkCassandraHelpers.Constants;

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
            this.cassandraCluster = new CassandraCluster(new CassandraClusterSettings());
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

        public void RemoveAllRunningProcesses()
        {
            MakeInConnection(
                RunningProcessesConstants.Keyspace,
                RunningProcessesConstants.ColumnFamily,
                connection => connection.DeleteRow(RunningProcessesConstants.Row)
                );
        }

        public string[] GetAllRunningProcesses()
        {
            string[] names = null;
            MakeInConnection(
                RunningProcessesConstants.Keyspace,
                RunningProcessesConstants.ColumnFamily,
                connection =>
                {
                    names = connection.GetRow(RunningProcessesConstants.Row).Select(x => x.Name).ToArray();
                });
            return names;
        }


        public void StartExecuting(string lockId, string processId)
        {
            MakeInConnection(
                ExecutingProcessesConstants.Keyspace,
                ExecutingProcessesConstants.ColumnFamily,
                connection => connection.AddColumn(lockId, new Column
                {
                    Name = processId,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Value = serializer.Serialize("start"),
                })
                );
        }

        public void StopExecuting(string lockId, string processId)
        {
            MakeInConnection(
                ExecutingProcessesConstants.Keyspace,
                ExecutingProcessesConstants.ColumnFamily,
                connection => connection.AddColumn(lockId, new Column
                {
                    Name = processId,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Value = serializer.Serialize("stop"),
                })
                );
        }

        public void WaitAllExecutingProcesses(string lockId)
        {
            while (true)
            {
                bool success = false;
                MakeInConnection(
                    ExecutingProcessesConstants.Keyspace,
                    ExecutingProcessesConstants.ColumnFamily,
                    connection =>
                    {
                        var row = connection.GetRow(lockId).ToArray();
                        success = row.All(x => serializer.Deserialize<string>(x.Value) == "stop");
                    });
                if(success)
                    return;
                //Console.WriteLine("Waiting...");
                Thread.Sleep(1000);
            }
        }

        public int GetExecutingProcessesCount(string lockId)
        {
            var result = 0;
            MakeInConnection(
                ExecutingProcessesConstants.Keyspace,
                ExecutingProcessesConstants.ColumnFamily,
                connection =>
                {
                    result = connection.GetCount(lockId);
                });
            return result;
        }

        public StartSignal WaitStartSignal(string processId, string oldLockId)
        {
            Console.WriteLine("Waiting for start signal");
            while(true)
            {
                StartSignal signal = null;
                MakeInConnection(
                    StartSignalCostants.Keyspace,
                    StartSignalCostants.ColumnFamily,
                    connection =>
                    {
                        var column = connection.GetRow(StartSignalCostants.Row).FirstOrDefault();
                        if(column != null)
                        {
                            signal = serializer.Deserialize<StartSignal>(column.Value);
                        }
                    });
                if(signal != null && signal.ProcessIds.Contains(processId) && signal.LockId != oldLockId)
                    return signal;
                Thread.Sleep(50);
            }
        }

        public void SendStartSignal(StartSignal signal)
        {
            MakeInConnection(
                StartSignalCostants.Keyspace,
                StartSignalCostants.ColumnFamily,
                connection => connection.AddColumn(StartSignalCostants.Row, new Column
                {
                    Name = StartSignalCostants.Column,
                    Timestamp = DateTime.UtcNow.Ticks,
                    Value = serializer.Serialize(signal),
                })
                );
        }

        public void RemoveStartSignal()
        {
            MakeInConnection(
                StartSignalCostants.Keyspace,
                StartSignalCostants.ColumnFamily,
                connection => connection.DeleteRow(StartSignalCostants.Row)
                );
        }

        public void WriteResults(string lockId, string processId, double[] times)
        {
            MakeInConnection(WriteResultsCostants.Keyspace, WriteResultsCostants.ColumnFamily, connection => connection.AddBatch(string.Format("{0}_{1}", lockId, processId), times.Select((value, index) => new Column
            {
                Name = index.ToString("000000000"),
                Timestamp = DateTime.UtcNow.Ticks,
                Value = serializer.Serialize(value),
            })));
        }

        public double[] GetResults(string lockId, string processId)
        {
            double[] result = {};
            MakeInConnection(WriteResultsCostants.Keyspace, WriteResultsCostants.ColumnFamily, connection => { result = connection.GetRow(string.Format("{0}_{1}", lockId, processId)).Select(x => serializer.Deserialize<double>(x.Value)).ToArray(); });
            return result;
        }

        public void SetLockId(string expId, int index, string id)
        {
            MakeInConnection(LockIdConstants.Keyspace, LockIdConstants.ColumnFamily, connection => connection.AddColumn(LockIdConstants.Row, new Column
            {
                Name = string.Format("{0}_{1}", expId, index),
                Timestamp = DateTime.UtcNow.Ticks,
                Value = serializer.Serialize(id),
            }));
        }

        public string GetLockId(string expId, int index)
        {
            while(true)
            {
                string result = null;
                MakeInConnection(LockIdConstants.Keyspace, LockIdConstants.ColumnFamily, connection =>
                {
                    Column column;
                    if (connection.TryGetColumn(LockIdConstants.Row, string.Format("{0}_{1}", expId, index), out column))
                        result = serializer.Deserialize<string>(column.Value);
                });
                if(result != null)
                    return result;
            }
        }

        public void MarkLockIdAsDone(string lockId, string processId)
        {
            MakeInConnection(LockIdConstants.Keyspace, LockIdConstants.ColumnFamily, connection => connection.AddColumn(lockId, new Column
            {
                Name = processId,
                Timestamp = DateTime.UtcNow.Ticks,
                Value = new byte[0],
            }));
        }

        public void WaitLockIdDone(string lockId, int processesCount)
        {
            while(true)
            {
                bool result = false;
                MakeInConnection(LockIdConstants.Keyspace, LockIdConstants.ColumnFamily, connection =>
                {
                    result = connection.GetCount(lockId) == processesCount;
                });
                if(result)
                    break;
            }
        }

        public ICassandraCluster GetCassandraCluster()
        {
            return cassandraCluster;
        }

        private void MakeInConnection(string keyspace, string columnFamily, Action<IColumnFamilyConnection> action)
        {
            action(GetCassandraCluster().RetrieveColumnFamilyConnection(keyspace, columnFamily));
        }

        private readonly Serializer serializer;
        private CassandraCluster cassandraCluster;
    }
}