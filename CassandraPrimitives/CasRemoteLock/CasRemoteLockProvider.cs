using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;

using Cassandra;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.CasRemoteLock
{
    public class CasRemoteLockProvider
    {
        private readonly string tableName;
        private readonly ISession session;
        private readonly TimeSpan lockTtl;
        private CasRemoteLockerPreparedStatements preparedStatements;
        private readonly TimeSpan prolongIntervalMs;

        public CasRemoteLockProvider(List<IPEndPoint> endpoints, string keyspaceName, string tableName, ConsistencyLevel consistencyLevel, TimeSpan lockTtl, TimeSpan prolongIntervalMs)
        {
            ThreadPool.SetMaxThreads(1000, 1000);
            ThreadPool.SetMinThreads(1000, 1000);

            this.tableName = tableName;
            this.lockTtl = lockTtl;
            this.prolongIntervalMs = prolongIntervalMs;
            var cluster = Cluster
                .Builder()
                .AddContactPoints(endpoints)
                .WithQueryOptions(new QueryOptions().SetConsistencyLevel(consistencyLevel))
                .Build();

            session = cluster.Connect(keyspaceName);
        }

        public CasRemoteLockProvider(List<IPEndPoint> endpoints, string keyspaceName, TimeSpan lockTtl, TimeSpan prolongIntervalMs) : this(endpoints, keyspaceName, "CASRemoteLock", ConsistencyLevel.Quorum, lockTtl, prolongIntervalMs)
        {
        }

        public CasRemoteLockProvider(ICassandraClusterSettings cassandraClusterSettings, CassandraRemoteLockImplementationSettings implementationSettings)
            : this(cassandraClusterSettings.Endpoints.Select(ep => new IPEndPoint(ep.Address, 9343)).ToList(), implementationSettings.ColumnFamilyFullName.KeyspaceName, "CASRemoteLock", ConsistencyLevel.Quorum, implementationSettings.LockTtl, implementationSettings.KeepLockAliveInterval)
        {
        }

        public void ActualiseTables()
        {
            //session.Execute(string.Format("DROP TABLE \"{0}\";", tableName), consistencyLevel);//TODO
            //if (session.Execute("SELECT * FROM system.schema_columnfamilies;", consistencyLevel).Any(row => row.GetValue<string>("columnfamily_name") == tableName))
            //    return;
            session.Execute(string.Format("CREATE TABLE IF NOT EXISTS \"{0}\" (", tableName) +
                                        "lock_id text PRIMARY KEY," +
                                        "owner text," +
                                        ");", ConsistencyLevel.All);
        }

        public void InitPreparedStatements()
        {
            preparedStatements = new CasRemoteLockerPreparedStatements(
                tryProlongStatement : session
                    .Prepare(string.Format("UPDATE \"{0}\" ", tableName) +
                             string.Format("USING TTL {0} ", (int)lockTtl.TotalSeconds) +
                             "SET owner = :Owner " +
                             "WHERE lock_id = :LockId " +
                             "IF owner = :Owner;"),
                tryAcquireStatement : session
                    .Prepare(string.Format("UPDATE \"{0}\" ", tableName) +
                             string.Format("USING TTL {0} ", (int)lockTtl.TotalSeconds) +
                             "SET owner = :Owner " +
                             "WHERE lock_id = :LockId " +
                             "IF owner = null;"),
                releaseStatement : session
                    .Prepare(string.Format("DELETE FROM \"{0}\" ", tableName) +
                             "WHERE lock_id = :LockId " +
                             "IF owner = :Owner"),
                getLockOwnerStatement : session
                    .Prepare(string.Format("SELECT owner FROM \"{0}\" ", tableName) +
                             "WHERE lock_id = :LockId;"));
        }

        public CasRemoteLocker CreateLocker()
        {
            return new CasRemoteLocker(session, prolongIntervalMs, preparedStatements);
        }
    }
}