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
    }
}