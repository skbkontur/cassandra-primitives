using System;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock.RemoteLocker;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.RemoteLocks
{
    public class CassandraRemoteLock : IRemoteLock
    {
        private readonly RemoteLocker remoteLocker;
        private readonly string lockId;
        private RemoteLock.IRemoteLock lastCassandraRemoteLock;

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
    }
}