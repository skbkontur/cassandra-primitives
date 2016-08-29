using System;
using System.Net;

using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public static class CassandraSessionProvider
    {
        private static readonly object locker = new object();
        private static ISession session;

        public static void InitOnce(IPEndPoint[] endpoints, ConsistencyLevel consistencyLevel, string keyspaceName)
        {
            lock (locker)
            {
                if (session == null)
                {
                    var cluster = Cluster
                        .Builder()
                        .AddContactPoints(endpoints)
                        .WithQueryOptions(new QueryOptions().SetConsistencyLevel(consistencyLevel))
                        .WithLoadBalancingPolicy(new TokenAwarePolicy(new RoundRobinPolicy()))
                        .Build();

                    session = cluster.Connect(keyspaceName);

                    session.Execute(string.Format("CREATE TABLE IF NOT EXISTS \"{0}\" (", CassandraCqlBaseLockOperationsPerformer.MainTableName) +
                                        "lock_id text," +
                                        "threshold text," +
                                        "thread_id text," +
                                        "PRIMARY KEY ((lock_id), threshold, thread_id)" +
                                        ");", ConsistencyLevel.All);

                    session.Execute(string.Format("CREATE TABLE IF NOT EXISTS \"{0}\" (", CassandraCqlBaseLockOperationsPerformer.MetadataTableName) +
                                        "key text PRIMARY KEY," +
                                        "lock_row_id text," +
                                        "lock_count int," +
                                        "previous_threshold bigint," +
                                        "probable_owner_thread_id text," +
                                        "timestamp bigint" +
                                        ");", ConsistencyLevel.All);
                }
            }
        }

        public static void InitOnce(IPEndPoint[] endpoints, string keyspaceName)
        {
            InitOnce(endpoints, ConsistencyLevel.Quorum, keyspaceName);
        }

        public static ISession Session
        {
            get
            {
                return session;
            }
        }

        public static PreparedStatements PrepareStatements()
        {
            return new PreparedStatements(
                writeThreadStatement : session.Prepare(string.Format("INSERT INTO \"{0}\" (lock_id, threshold, thread_id) VALUES (:LockId, :Threshold, :ThreadId) USING TIMESTAMP :Timestamp AND TTL :Ttl",
                                                                     CassandraCqlBaseLockOperationsPerformer.MainTableName)),
                deleteThreadStatement : session.Prepare(string.Format("DELETE FROM \"{0}\" USING TIMESTAMP :Timestamp WHERE lock_id = :LockId AND threshold = :Threshold AND thread_id = :ThreadId;",
                                                                      CassandraCqlBaseLockOperationsPerformer.MainTableName)),
                threadAliveStatement : session.Prepare(string.Format("SELECT COUNT(*) FROM \"{0}\" WHERE lock_id = :LockId AND threshold = :Threshold AND thread_id = :ThreadId;",
                                                                     CassandraCqlBaseLockOperationsPerformer.MainTableName)),
                searchThreadsStatement : session.Prepare(string.Format("SELECT thread_id FROM \"{0}\" WHERE lock_id = :LockId AND threshold >= :Threshold;",
                                                                       CassandraCqlBaseLockOperationsPerformer.MainTableName)),
                writeLockMetadataStatement : session.Prepare(string.Format("INSERT INTO \"{0}\" " +
                                                                           "(key, lock_row_id, lock_count, previous_threshold, probable_owner_thread_id, timestamp) " +
                                                                           "VALUES (:Key, :LockRowId, :LockCount, :PreviousThreshold, :ProbableOwnerThreadId, :MetadataTimestamp) " +
                                                                           "USING TIMESTAMP :Timestamp AND TTL :Ttl",
                                                                           CassandraCqlBaseLockOperationsPerformer.MetadataTableName)),
                tryGetLockMetadataStatement : session.Prepare(string.Format("SELECT * FROM \"{0}\" WHERE key = :Key;",
                                                                            CassandraCqlBaseLockOperationsPerformer.MetadataTableName))
                );
        }
    }
}