using System;

using BenchmarkCassandraHelpers;

namespace LockProcess
{
    public class ProcessParameters
    {
        public ProcessParameters(string[] args)
        {
            ProcessId = args[0];
            LockId = args[1];
            Keyspace = args[2];
            ColumnFamily = args[3];
            LocksCount = int.Parse(args[4]);
            LockType lockType;
            Enum.TryParse(args[5], true, out lockType);
            LockType = lockType;
        }

        public string ProcessId { get; private set; }
        public string LockId { get; private set; }
        public string Keyspace { get; private set; }
        public string ColumnFamily { get; private set; }
        public int LocksCount { get; private set; }
        public LockType LockType { get; private set; }
    }
}