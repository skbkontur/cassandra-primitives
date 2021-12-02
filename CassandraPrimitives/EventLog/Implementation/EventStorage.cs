using System.Collections.Generic;
using System.Linq;

using GroBuf;

using SkbKontur.Cassandra.Primitives.EventLog.Primitives;
using SkbKontur.Cassandra.Primitives.Storages.Primitives;
using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Connections;

namespace SkbKontur.Cassandra.Primitives.EventLog.Implementation
{
    internal class EventStorage : IEventStorage
    {
        public EventStorage(
            ColumnFamilyFullName columnFamilyFullName,
            IEventLogPointerCreator eventLogPointerCreator,
            ICassandraCluster cassandraCluster,
            ISerializer serializer)
        {
            this.eventLogPointerCreator = eventLogPointerCreator;
            this.serializer = serializer;
            columnFamilyConnection = cassandraCluster.RetrieveColumnFamilyConnection(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
        }

        public void Write(EventLogRecord[] events, long timestamp, int? ttl = null)
        {
            var columns = events
                          .Select(x => new KeyValuePair<EventPointer, EventLogRecord>(eventLogPointerCreator.Create(x.StorageElement.EventInfo), x))
                          .GroupBy(x => x.Key.RowKey)
                          .Select(eventsGropedByRow =>
                                      new KeyValuePair<string, IEnumerable<Column>>(
                                          eventsGropedByRow.Key,
                                          eventsGropedByRow.Select(x => new Column
                                              {
                                                  Name = x.Key.ColumnName,
                                                  Value = serializer.Serialize(x.Value),
                                                  Timestamp = timestamp,
                                                  TTL = ttl
                                              }))).ToArray();
            columnFamilyConnection.BatchInsert(columns);
        }

        public void Delete(EventInfo[] eventInfos, long timestamp)
        {
            var eventsForDelete = eventInfos
                                  .Select(x => eventLogPointerCreator.Create(x))
                                  .GroupBy(x => x.RowKey)
                                  .Select(eventsGropedByRow =>
                                              new KeyValuePair<string, IEnumerable<string>>(
                                                  eventsGropedByRow.Key,
                                                  eventsGropedByRow.Select(x => x.ColumnName)))
                                  .ToArray();
            columnFamilyConnection.BatchDelete(eventsForDelete, timestamp);
        }

        private readonly IEventLogPointerCreator eventLogPointerCreator;
        private readonly ISerializer serializer;
        private readonly IColumnFamilyConnection columnFamilyConnection;
    }
}