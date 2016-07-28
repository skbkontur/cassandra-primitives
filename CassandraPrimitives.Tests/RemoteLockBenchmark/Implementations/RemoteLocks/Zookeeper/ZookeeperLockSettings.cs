using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.RemoteLocks.Zookeeper
{
    public class ZookeeperLockSettings
    {
        public ZookeeperLockSettings(string connectionString, string @namespace, TimeSpan lockTtl)
        {
            ConnectionString = connectionString;
            Namespace = @namespace;
            LockTtl = lockTtl;
        }

        public string ConnectionString { get; private set; }
        public string Namespace { get; private set; }
        public TimeSpan LockTtl { get; private set; }
    }
}