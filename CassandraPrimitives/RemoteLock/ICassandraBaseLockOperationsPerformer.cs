using System;

using JetBrains.Annotations;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    internal interface ICassandraBaseLockOperationsPerformer
    {
        void WriteThread([NotNull] string lockRowId, long threshold, [NotNull] string threadId, TimeSpan ttl);

        void DeleteThread([NotNull] string lockRowId, long threshold, [NotNull] string threadId);

        bool ThreadAlive([NotNull] string lockRowId, long? threshold, [NotNull] string threadId);

        [NotNull]
        string[] SearchThreads([NotNull] string lockRowId, long? threshold);

        void WriteLockMetadata([NotNull] NewLockMetadata newLockMetadata, long oldLockMetadataTimestamp);

        [CanBeNull]
        LockMetadata TryGetLockMetadata([NotNull] string lockId);
    }
}