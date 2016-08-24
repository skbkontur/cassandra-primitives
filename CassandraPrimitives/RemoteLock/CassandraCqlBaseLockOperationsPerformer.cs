using System;
using System.Linq;
using System.Net;
using System.Threading;

using Cassandra;

using JetBrains.Annotations;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class CassandraCqlBaseLockOperationsPerformer : ICassandraBaseLockOperationsPerformer
    {
        public CassandraCqlBaseLockOperationsPerformer(IPEndPoint[] endpoints, int cqlPort, CassandraRemoteLockImplementationSettings settings)
        {
            endpoints = endpoints.Select(ep => new IPEndPoint(ep.Address, cqlPort)).ToArray();
            CassandraSessionProvider.InitOnce(endpoints, ConsistencyLevel.Quorum, settings.ColumnFamilyFullName.KeyspaceName);
            session = CassandraSessionProvider.Session;
            timestampProvider = settings.TimestampProvider;
            lockTtl = settings.LockTtl;
            lockMetadataTtl = settings.LockMetadataTtl;
        }

        public void WriteThread(string lockRowId, long threshold, string threadId, TimeSpan ttl)
        {
            session.Execute(string.Format(
                "INSERT INTO \"{0}\" (lock_id, threshold, thread_id) VALUES ('{1}', '{2}', '{3}') USING TIMESTAMP {4} AND TTL {5}",
                MainTableName,
                lockRowId,
                ThresholdToString(threshold),
                threadId,
                GetNowTicks(),
                ttl.TotalSeconds));
        }

        public void DeleteThread(string lockRowId, long threshold, string threadId)
        {
            session.Execute(string.Format(
                "DELETE FROM \"{0}\" USING TIMESTAMP {1} WHERE lock_id = '{2}' AND threshold = '{3}' AND thread_id = '{4}';",
                MainTableName,
                GetNowTicks(),
                lockRowId,
                ThresholdToString(threshold),
                threadId));
        }

        public bool ThreadAlive(string lockRowId, long? threshold, string threadId)
        {
            var rowSet = session.Execute(string.Format(
                "SELECT COUNT(*) FROM \"{0}\" WHERE lock_id = '{1}' AND threshold = '{2}' AND thread_id = '{3}';",
                MainTableName,
                lockRowId,
                ThresholdToString(threshold),
                threadId)).ToList();
            return rowSet.Single().GetValue<long>("count") > 0;
        }

        public string[] SearchThreads(string lockRowId, long? threshold)
        {
            var rowSet = session
                .Execute(string.Format(
                    "SELECT thread_id FROM \"{0}\" WHERE lock_id = '{1}' AND threshold > '{2}';",
                    MainTableName,
                    lockRowId,
                    ThresholdToString(threshold - lockTtl.Multiply(2).Ticks)))
                .ToList();
            return rowSet
                .Select(row => row.GetValue<string>("thread_id"))
                .Distinct()
                .ToArray();
        }

        public void WriteLockMetadata(NewLockMetadata newLockMetadata, long oldLockMetadataTimestamp)
        {
            var newTimestamp = Math.Max(GetNowTicks(), oldLockMetadataTimestamp + 1);
            var key = newLockMetadata.LockId.ToLockMetadataRowKey();
            session.Execute(string.Format(
                "INSERT INTO \"{0}\" (key, lock_row_id, lock_count, previous_threshold, probable_owner_thread_id, timestamp) VALUES ('{1}', '{2}', {3}, {4}, '{5}', {6}) USING TIMESTAMP {7} AND TTL {8}",
                MetadataTableName,
                key,
                newLockMetadata.LockRowId,
                newLockMetadata.LockCount,
                newLockMetadata.Threshold,
                newLockMetadata.OwnerThreadId,
                newTimestamp,
                newTimestamp,
                lockMetadataTtl.TotalSeconds));
        }

        public LockMetadata TryGetLockMetadata(string lockId)
        {
            var key = lockId.ToLockMetadataRowKey();
            var rowSet = session
                .Execute(string.Format(
                    "SELECT * FROM \"{0}\" WHERE key = '{1}';",
                    MetadataTableName,
                    key))
                .ToList();
            if(rowSet.Count == 0)
                return null;
            var row = rowSet.Single();
            return new LockMetadata(lockId, row.GetValue<string>("lock_row_id"), row.GetValue<int>("lock_count"), row.GetValue<long>("previous_threshold"), row.GetValue<string>("probable_owner_thread_id"), row.GetValue<long>("timestamp"));
        }

        [CanBeNull]
        private static string ThresholdToString(long? threshold)
        {
            return threshold == null ? null : threshold.Value.ToString("D20");
        }

        private long GetNowTicks()
        {
            var ticks = timestampProvider.GetNowTicks();
            while(true)
            {
                var last = Interlocked.Read(ref lastTicks);
                var cur = Math.Max(ticks, last + 1);
                if(Interlocked.CompareExchange(ref lastTicks, cur, last) == last)
                    return cur;
            }
        }

        private readonly ISession session;
        public const string MainTableName = "CQLRemoteLockMain";
        public const string MetadataTableName = "CQLRemoteLockMetadata";
        private readonly ITimestampProvider timestampProvider;
        private readonly TimeSpan lockTtl;
        private readonly TimeSpan lockMetadataTtl;
        private long lastTicks;
    }
}