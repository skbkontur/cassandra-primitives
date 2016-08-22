using System;

using ZooKeeper.Recipes;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.Zookeeper
{
    public class ZookeeperRemoteLock : IRemoteLock
    {
        public ZookeeperRemoteLock(DistributedLock distributedLock)
        {
            this.distributedLock = distributedLock;
        }

        public IDisposable Acquire()
        {
            distributedLock.Acquire();
            return new AcquiredLock(distributedLock);
        }

        public bool TryAcquire(out IDisposable remoteLock)
        {
            if (distributedLock.Acquire(TimeSpan.Zero))
            {
                remoteLock = new AcquiredLock(distributedLock);
                return true;
            }
            remoteLock = null;
            return false;
        }

        public void Release()
        {
            distributedLock.Release();
        }

        private readonly DistributedLock distributedLock;

        private class AcquiredLock : IDisposable
        {
            public AcquiredLock(DistributedLock distributedLock)
            {
                this.distributedLock = distributedLock;
            }

            public void Dispose()
            {
                distributedLock.Release();
            }

            private readonly DistributedLock distributedLock;
        }
    }
}