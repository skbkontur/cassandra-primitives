﻿using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver.RemoteLocks
{
    public class ZookeeperLockSettings
    {
        public ZookeeperLockSettings(string connectionString, string @namespace, TimeSpan lockTtl)
        {
            ConnectionString = connectionString;
            Namespace = @namespace;
            LockTtl = lockTtl;
        }

        public string ConnectionString { get; set; }
        public string Namespace { get; set; }
        public TimeSpan LockTtl { get; set; }
    }
}