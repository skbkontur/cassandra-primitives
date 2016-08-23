using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.CasRemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.CASCassandra
{
    public class CassandraCasRemoteLock : IRemoteLock
    {
        private readonly CasRemoteLocker remoteLocker;
        private readonly string lockId;

        public CassandraCasRemoteLock(CasRemoteLocker remoteLocker, string lockId)
        {
            this.remoteLocker = remoteLocker;
            this.lockId = lockId;
        }

        public IDisposable Acquire()
        {
            throw new NotSupportedException();
        }

        public bool TryAcquire(out IDisposable remoteLock)
        {
            return remoteLocker.TryAcquire(lockId, out remoteLock);
        }

        public void Release()
        {
            throw new NotSupportedException();
        }
    }
}