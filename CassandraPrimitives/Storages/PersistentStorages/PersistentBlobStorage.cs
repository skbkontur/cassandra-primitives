using System;
using System.Collections.Generic;
using System.Linq;

using GroBuf;

using MoreLinq;

using SkbKontur.Cassandra.Primitives.Storages.Exceptions;
using SkbKontur.Cassandra.Primitives.Storages.Primitives;
using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Connections;

namespace SkbKontur.Cassandra.Primitives.Storages.PersistentStorages
{
    public class PersistentBlobStorage<T, TId> : IPersistentStorage<T, TId>
        where T : class
    {
        public PersistentBlobStorage(ColumnFamilyFullName columnFamilyFullName, ICassandraCluster cassandraCluster, ISerializer serializer, ICassandraObjectIdConverter<T, TId> cassandraObjectIdConverter)
        {
            this.cassandraCluster = cassandraCluster;
            this.serializer = serializer;
            this.cassandraObjectIdConverter = cassandraObjectIdConverter;
            keyspaceName = columnFamilyFullName.KeyspaceName;
            columnFamilyName = columnFamilyFullName.ColumnFamilyName;
        }

        public void Write(T[] objects, DateTime timestamp)
        {
            const int batchSize = 1000;

            foreach (var @object in objects)
                cassandraObjectIdConverter.CheckObjectIdentity(@object);
            foreach (var batch in objects.Batch(batchSize, Enumerable.ToArray))
                WriteInternal(batch, timestamp);
        }

        public void Write(T data, DateTime timestamp)
        {
            cassandraObjectIdConverter.CheckObjectIdentity(data);
            MakeInConnection(conn => conn.AddColumn(
                cassandraObjectIdConverter.GetRowKey(data),
                CreateColumn(data, timestamp))
                );
        }

        public T[] ReadQuiet(TId[] ids)
        {
            CheckObjectIdentitiesValidness(ids);

            var cassandraIds = ids.Select(x => cassandraObjectIdConverter.IdToRowKey(x)).ToArray();

            var rows = new List<KeyValuePair<string, Column[]>>();
            foreach (var batchIds in cassandraIds.Batch(1000, Enumerable.ToArray))
                MakeInConnection(connection => rows.AddRange(connection.GetRowsExclusive(batchIds, null, maximalColumnsCount)));
            var rowsDict = rows.ToDictionary(row => row.Key);
            var result = new T[cassandraIds.Length];
            for (var i = 0; i < cassandraIds.Length; i++)
            {
                var id = cassandraIds[i];
                if (rowsDict.TryGetValue(id, out var row))
                    result[i] = Read(row.Value);
            }
            return result;
        }

        public void Delete(TId id, DateTime timestamp)
        {
            CheckObjectIdentityValidness(id);

            var cassandraId = cassandraObjectIdConverter.IdToRowKey(id);
            MakeInConnection(connection =>
                {
                    var columns = connection.GetColumns(cassandraId, null, maximalColumnsCount);
                    connection.DeleteBatch(cassandraId, columns.Select(col => col.Name), timestamp.Ticks);
                });
        }

        public void Delete(TId[] ids, DateTime timestamp)
        {
            CheckObjectIdentitiesValidness(ids);
            foreach (var batchIds in ids.Batch(1000, Enumerable.ToArray))
                DeleteInternal(batchIds, timestamp);
        }

        private void DeleteInternal(TId[] ids, DateTime? timestamp)
        {
            MakeInConnection(
                connection => connection.DeleteRows(ids.Select(x => cassandraObjectIdConverter.IdToRowKey(x)).ToArray(), timestamp?.Ticks));
        }

        public void Update(TId id, Action<T> updateAction, DateTime timestamp)
        {
            var previousObject = TryRead(id);
            if (previousObject == null)
                throw new ObjectNotFoundException(cassandraObjectIdConverter.IdToRowKey(id));
            updateAction(previousObject);
            Write(previousObject, timestamp);
        }

        public T TryRead(TId id)
        {
            CheckObjectIdentityValidness(id);
            T temp = null;
            MakeInConnection(
                conn =>
                    {
                        if (conn.TryGetColumn(cassandraObjectIdConverter.IdToRowKey(id), "Content", out var column))
                            temp = serializer.Deserialize<T>(column.Value);
                    });
            var result = temp;
            return result;
        }

        public T[] TryRead(TId[] ids)
        {
            CheckObjectIdentitiesValidness(ids);
            if (ids.Length == 0)
                return new T[0];
            var rowKeys = ids.Select(x => cassandraObjectIdConverter.IdToRowKey(x)).ToArray();
            return TryReadInternal(rowKeys);
        }

        public TId[] GetIds(TId exclusiveStartId, int count)
        {
            TId[] result = null;
            MakeInConnection(conn => { result = conn.GetKeys(cassandraObjectIdConverter.IdToRowKeyDef(exclusiveStartId), count).Select(x => cassandraObjectIdConverter.RowKeyToId(x)).ToArray(); });
            return result;
        }

        private void WriteInternal(T[] objects, DateTime timestamp)
        {
            var batchToInsert = objects
                .Select(x => CreateObjectRow(x, timestamp))
                .ToList();

            MakeInConnection(connection => connection.BatchInsert(batchToInsert));
        }

        private KeyValuePair<string, IEnumerable<Column>> CreateObjectRow(T @object, DateTime timestamp)
        {
            return new KeyValuePair<string, IEnumerable<Column>>(
                cassandraObjectIdConverter.GetRowKey(@object),
                new[]
                    {
                        CreateColumn(@object, timestamp)
                    });
        }

        private static void CheckObjectIdentityValidness(TId id)
        {
            if (id == null)
                throw new ArgumentNullException("id");
        }

        private static void CheckObjectIdentitiesValidness(TId[] ids)
        {
            if (ids == null)
                throw new ArgumentNullException("ids");
        }

        private T[] TryReadInternal(string[] rowKeys)
        {
            if (rowKeys == null) throw new ArgumentNullException("rowKeys");
            if (rowKeys.Length == 0) return new T[0];
            var rows = new List<KeyValuePair<string, Column[]>>();
            foreach (var batchIds in rowKeys.Batch(1000, Enumerable.ToArray))
                MakeInConnection(connection => rows.AddRange(connection.GetRowsExclusive(batchIds, null, maximalColumnsCount)));
            var rowsDict = rows.ToDictionary(row => row.Key);
            return rowKeys.Where(rowsDict.ContainsKey).Select(id => Read(rowsDict[id].Value)).Where(obj => obj != null).ToArray();
        }

        private T Read(IEnumerable<Column> columns)
        {
            return columns.Where(column => column.Name == "Content").Select(column => serializer.Deserialize<T>(column.Value)).FirstOrDefault();
        }

        private Column CreateColumn(T data, DateTime timestamp)
        {
            var content = serializer.Serialize(data);
            return new Column
                {
                    Name = "Content",
                    Value = content,
                    Timestamp = timestamp.Ticks,
                };
        }

        private void MakeInConnection(Action<IColumnFamilyConnection> action)
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(keyspaceName, columnFamilyName);
            action(connection);
        }

        private const int maximalColumnsCount = 1000;
        private readonly ICassandraCluster cassandraCluster;
        private readonly ISerializer serializer;
        private readonly ICassandraObjectIdConverter<T, TId> cassandraObjectIdConverter;
        private readonly string keyspaceName;
        private readonly string columnFamilyName;
    }
}