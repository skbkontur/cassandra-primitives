using System;

using BenchmarkCassandraHelpers;

namespace ProcessesBasedRemoteLockBenchmark
{
    internal class BenchmarkParameters
    {
        public BenchmarkParameters(int processesCount, int locksCount, LockType lockType)
        {
            ProcessesCount = processesCount;
            LocksCount = locksCount;
            Keyspace = Guid.NewGuid().ToString("N");
            ColumnFamily = Guid.NewGuid().ToString("N");
            LockType = lockType;
            LockId = Guid.NewGuid().ToString();
        }

        public int ProcessesCount { get; private set; }
        public int LocksCount { get; private set; }
        public string Keyspace { get; private set; }
        public string ColumnFamily { get; private set; }
        public LockType LockType { get; private set; }
        public string LockId { get; private set; }
    }
}