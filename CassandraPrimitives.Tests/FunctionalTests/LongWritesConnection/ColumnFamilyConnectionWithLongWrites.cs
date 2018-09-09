using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.LongWritesConnection
{
    public class ColumnFamilyConnectionWithLongWrites : IColumnFamilyConnection
    {
        public ColumnFamilyConnectionWithLongWrites(IColumnFamilyConnection columnFamilyConnection, ColumnFamilyFullName columnFamilyFullName)
            : this(columnFamilyConnection, columnFamilyFullName, TimeSpan.FromMilliseconds(10))
        {
        }

        public ColumnFamilyConnectionWithLongWrites(IColumnFamilyConnection columnFamilyConnection, ColumnFamilyFullName columnFamilyFullName, TimeSpan timeout)
            : this(columnFamilyConnection, columnFamilyFullName, timeout, 20)
        {
        }

        public ColumnFamilyConnectionWithLongWrites(IColumnFamilyConnection columnFamilyConnection, ColumnFamilyFullName columnFamilyFullName, TimeSpan timeout, int attempts)
        {
            this.columnFamilyConnection = columnFamilyConnection;
            this.columnFamilyFullName = columnFamilyFullName;
            this.timeout = timeout;
            this.attempts = attempts;
        }

        public bool IsRowExist(string key)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.IsRowExist(key);
        }

        public void DeleteRows(string[] keys, long? timestamp = null, int batchSize = 1000)
        {
            BeforeExecuteCommand();
            columnFamilyConnection.DeleteRows(keys, timestamp, batchSize);
        }

        public void DeleteRow(string key, long? timestamp = null)
        {
            BeforeExecuteCommand();
            columnFamilyConnection.DeleteRow(key, timestamp);
        }

        public void DeleteColumn(string key, string columnName, long? timestamp = null)
        {
            BeforeExecuteCommand();
            columnFamilyConnection.DeleteColumn(key, columnName, timestamp);
        }

        public void AddColumn(string key, Column column)
        {
            BeforeExecuteCommand();
            columnFamilyConnection.AddColumn(key, column);
        }

        public void AddColumn(Func<int, KeyColumnPair<string>> createKeyColumnPair)
        {
            columnFamilyConnection.AddColumn((attempt) =>
                {
                    var result = createKeyColumnPair(attempt);
                    BeforeExecuteCommand(false);
                    return result;
                });
        }

        public Column GetColumn(string key, string columnName)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetColumn(key, columnName);
        }

        public bool TryGetColumn(string key, string columnName, out Column result)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.TryGetColumn(key, columnName, out result);
        }

        public void DeleteBatch(string key, IEnumerable<string> columnNames, long? timestamp = null)
        {
            BeforeExecuteCommand();
            columnFamilyConnection.DeleteBatch(key, columnNames, timestamp);
        }

        public void AddBatch(string key, IEnumerable<Column> columns)
        {
            BeforeExecuteCommand();
            columnFamilyConnection.AddBatch(key, columns);
        }

        public void AddBatch(Func<int, KeyColumnsPair<string>> createKeyColumnsPair)
        {
            columnFamilyConnection.AddBatch(attempt =>
                {
                    var result = createKeyColumnsPair(attempt);
                    BeforeExecuteCommand(false);
                    return result;
                });
        }

        public void BatchInsert(IEnumerable<KeyValuePair<string, IEnumerable<Column>>> data)
        {
            BeforeExecuteCommand();
            columnFamilyConnection.BatchInsert(data);
        }

        public void BatchDelete(IEnumerable<KeyValuePair<string, IEnumerable<string>>> data, long? timestamp = null)
        {
            BeforeExecuteCommand();
            columnFamilyConnection.BatchDelete(data, timestamp);
        }

        public List<KeyValuePair<string, Column[]>> GetRows(IEnumerable<string> keys, string[] columnNames)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetRows(keys, columnNames);
        }

        public List<KeyValuePair<string, Column[]>> GetRegion(IEnumerable<string> keys, string startColumnName, string finishColumnName, int limitPerRow)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetRegion(keys, startColumnName, finishColumnName, limitPerRow);
        }

        public List<KeyValuePair<string, Column[]>> GetRowsExclusive(IEnumerable<string> keys, string exclusiveStartColumnName, int count)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetRowsExclusive(keys, exclusiveStartColumnName, count);
        }

        public void Truncate()
        {
            BeforeExecuteCommand();
            columnFamilyConnection.Truncate();
        }

        public Column[] GetColumns(string key, string exclusiveStartColumnName, int count, bool reversed)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetColumns(key, exclusiveStartColumnName, count, reversed);
        }

        public Column[] GetColumns(string key, string startColumnName, string endColumnName, int count, bool reversed = false)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetColumns(key, startColumnName, endColumnName, count, reversed);
        }

        public Column[] GetColumns(string key, string[] columnNames)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetColumns(key, columnNames);
        }

        public IEnumerable<Column> GetRow(string key, int batchSize = 1000)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetRow(key, batchSize);
        }

        public IEnumerable<Column> GetRow(string key, string exclusiveStartColumnName, int batchSize = 1000)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetRow(key, exclusiveStartColumnName, batchSize);
        }

        public string[] GetKeys(string exclusiveStartKey, int count)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetKeys(exclusiveStartKey, count);
        }

        public IEnumerable<string> GetKeys(int batchSize = 1000)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetKeys(batchSize);
        }

        public int GetCount(string key)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetCount(key);
        }

        public Dictionary<string, int> GetCounts(IEnumerable<string> keys)
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetCounts(keys);
        }

        public ICassandraConnectionParameters GetConnectionParameters()
        {
            BeforeExecuteCommand();
            return columnFamilyConnection.GetConnectionParameters();
        }

        private void BeforeExecuteCommand(bool withAttempts = true)
        {
            count++;
            var stackTrace = new StackTrace();
            Console.WriteLine("{3}. {1}.{2}.{0}()", stackTrace.GetFrame(1).GetMethod().Name, columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName, count);
            Thread.Sleep(TimeSpan.FromTicks(timeout.Ticks * (withAttempts ? attempts : 1)));
        }

        private static volatile int count = 0;

        private readonly IColumnFamilyConnection columnFamilyConnection;
        private readonly ColumnFamilyFullName columnFamilyFullName;
        private readonly TimeSpan timeout;
        private readonly int attempts;
    }
}