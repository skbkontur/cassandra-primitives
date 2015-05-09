using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

using MoreLinq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.ExpirationJudge;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.ExpiryChecker
{
    public class ExpiryChecker : IExpiryChecker
    {
        public ExpiryChecker(IExpirationObjectStateUpdater expirationObjectStateUpdater, ICassandraCluster cassandraCluster)
        {
            this.expirationObjectStateUpdater = expirationObjectStateUpdater;
            this.cassandraCluster = cassandraCluster;
            states = new ConcurrentDictionary<ExpiringObjectMeta, ExpiringObjectState>();
        }

        public void AddNewEntries(ExpiringObjectMeta[] metas)
        {
            foreach(var meta in metas)
                states.TryAdd(meta, null);
            Console.WriteLine("Added {0} metas", metas.Length);
        }

        public void Check()
        {
            var currentStates = states.ToArray();
            var keyspaceColumnFamilyGroups = currentStates.GroupBy(x => Tuple.Create(x.Key.Keyspace, x.Key.ColumnFamily)).ToArray();
            keyspaceColumnFamilyGroups.ForEach(x => ProcessColumnFamily(x.Key.Item1, x.Key.Item2, x.ToArray()));
        }

        private void ProcessColumnFamily(string keyspace, string columnFamily, KeyValuePair<ExpiringObjectMeta, ExpiringObjectState>[] columnFamilyObjects)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(keyspace, columnFamily);
            var rowGroups = columnFamilyObjects.GroupBy(x => x.Key.Row).ToArray();
            rowGroups.ForEach(x => ProcessRow(connection, x.Key, x.ToArray()));
        }

        private void ProcessRow(IColumnFamilyConnection connection, string rowName, KeyValuePair<ExpiringObjectMeta, ExpiringObjectState>[] rowObjects)
        {
            var rowDict = rowObjects.ToDictionary(x => x.Key.Column, x => x);
            var columnNames = rowDict.Keys.ToArray();
            var columns = connection.GetColumns(rowName, columnNames);
            var columnsDict = columns.ToDictionary(x => x.Name, x => x);
            var expiredColumns = new List<string>();
            rowObjects.ForEach(x =>
            {
                Column column;
                columnsDict.TryGetValue(x.Key.Column, out column);
                bool isExpired;
                ProcessObject(x.Key, x.Value, column, out isExpired);
                if(isExpired)
                    expiredColumns.Add(x.Key.Column);
            });
            if(expiredColumns.Count > 0)
                connection.DeleteBatch(rowName, expiredColumns, columns.Max(x => x.Timestamp) + 1);
        }

        private void ProcessObject(ExpiringObjectMeta meta, ExpiringObjectState currentState, Column column, out bool isExpired)
        {
            ExpiringObjectState newState;
            if(column != null)
                newState = expirationObjectStateUpdater.Update(currentState, column.Timestamp);
            else
                newState = expirationObjectStateUpdater.Update(currentState, null);
            if(newState == null)
            {
                isExpired = false;
                states.TryRemove(meta, out currentState);
            }
            else if(newState.IsExpired)
            {
                isExpired = true;
                states.TryRemove(meta, out currentState);
            }
            else
            {
                isExpired = false;
                states.TryUpdate(meta, newState, currentState);
            }
        }

        private readonly IExpirationObjectStateUpdater expirationObjectStateUpdater;
        private readonly ICassandraCluster cassandraCluster;
        private readonly ConcurrentDictionary<ExpiringObjectMeta, ExpiringObjectState> states;
    }
}