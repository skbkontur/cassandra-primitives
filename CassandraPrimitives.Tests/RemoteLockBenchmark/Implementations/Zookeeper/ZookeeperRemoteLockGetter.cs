using System;

using Kontur.Logging;

using ZooKeeper.Recipes;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.Zookeeper
{
    public class ZookeeperRemoteLockGetter : IRemoteLockGetter, IDisposable
    {
        public ZookeeperRemoteLockGetter(ZookeeperLockSettings zookeeperLockSettings)
        {
            this.zookeeperLockSettings = zookeeperLockSettings;
        }

        public IRemoteLock Get(string lockId)
        {
            var distributedLock = new DistributedLock(zookeeperLockSettings.ConnectionString, zookeeperLockSettings.Namespace, "/locks/" + lockId, zookeeperLockSettings.LockTtl, new FakeLog());
            return new ZookeeperRemoteLock(distributedLock);
        }

        public void Dispose()
        {
        }

        private readonly ZookeeperLockSettings zookeeperLockSettings;
    }
}