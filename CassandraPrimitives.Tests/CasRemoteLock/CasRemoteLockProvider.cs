using System;
using System.Collections.Generic;
using System.Net;

using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.CasRemoteLock
{
    public class CasRemoteLockProvider
    {
        private readonly string tableName;
        private readonly ISession session;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly TimeSpan lockTtl;
        private PreparedStatement tryProlongStatement, tryAcquireStatement, releaseStatement;

        public CasRemoteLockProvider(List<IPEndPoint> endpoints, string keyspaceName, string tableName, ConsistencyLevel consistencyLevel, TimeSpan lockTtl)
        {
            this.tableName = tableName;
            this.consistencyLevel = consistencyLevel;
            this.lockTtl = lockTtl;
            var cluster = Cluster
                .Builder()
                .AddContactPoints(endpoints)
                .WithQueryOptions(new QueryOptions().SetConsistencyLevel(consistencyLevel))
                .Build();

            session = cluster.Connect(keyspaceName);
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
            tryProlongStatement = session
                .Prepare(string.Format("UPDATE \"{0}\" ", tableName) +
                         string.Format("USING TTL {0} ", lockTtl.Seconds) +
                         "SET owner = :Owner " +
                         "WHERE lock_id = :LockId " +
                         "IF owner = :Owner;");
            tryAcquireStatement = session
                .Prepare(string.Format("UPDATE \"{0}\" ", tableName) +
                         string.Format("USING TTL {0} ", lockTtl.Seconds) +
                         "SET owner = :Owner " +
                         "WHERE lock_id = :LockId " +
                         "IF owner = null;");
            releaseStatement = session
                .Prepare(string.Format("DELETE FROM \"{0}\" ", tableName) +
                         "WHERE lock_id = :LockId " +
                         "IF owner = :Owner");
        }

        public CasRemoteLocker CreateLocker()
        {
            return new CasRemoteLocker(session, lockTtl, tryProlongStatement, tryAcquireStatement, releaseStatement);
        }
    }
}