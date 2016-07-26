using System;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.RemoteLocks.Cassandra
{
    public class CassandraRemoteLock : IRemoteLock
    {
        public CassandraRemoteLock(RemoteLocker remoteLocker, string lockId)
        {
            this.remoteLocker = remoteLocker;
            this.lockId = lockId;
        }

        public IDisposable Acquire()
        {
            lastCassandraRemoteLock = remoteLocker.Lock(lockId);
            return lastCassandraRemoteLock;
        }

        public bool TryAcquire(out IDisposable remoteLock)
        {
            var result = remoteLocker.TryGetLock(lockId, out lastCassandraRemoteLock);
            remoteLock = lastCassandraRemoteLock;
            return result;
        }

        public void Release()
        {
            remoteLocker.Lock(lockId).Dispose();
        }

        private readonly RemoteLocker remoteLocker;
        private readonly string lockId;
        private RemoteLock.IRemoteLock lastCassandraRemoteLock;
    }
}