using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Connections;

namespace CassandraPrimitives.Tests.FunctionalTests.LongWritesConnection
{
    public class KeyspaceConnectionWithLongWrites : IKeyspaceConnection
    {
        public KeyspaceConnectionWithLongWrites(IKeyspaceConnection keyspaceConnection)
        {
            this.keyspaceConnection = keyspaceConnection;
        }

        public void RemoveColumnFamily(string columnFamily)
        {
            keyspaceConnection.RemoveColumnFamily(columnFamily);
        }

        public void AddColumnFamily(string columnFamilyName)
        {
            keyspaceConnection.AddColumnFamily(columnFamilyName);
        }

        public void UpdateColumnFamily(ColumnFamily columnFamily)
        {
            keyspaceConnection.UpdateColumnFamily(columnFamily);
        }

        public void AddColumnFamily(ColumnFamily columnFamily)
        {
            keyspaceConnection.AddColumnFamily(columnFamily);
        }

        public Keyspace DescribeKeyspace()
        {
            return keyspaceConnection.DescribeKeyspace();
        }

        private readonly IKeyspaceConnection keyspaceConnection;
    }
}