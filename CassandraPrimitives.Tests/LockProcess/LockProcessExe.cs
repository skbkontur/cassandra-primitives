using System;
using System.Collections.Generic;
using System.Diagnostics;

using BenchmarkCassandraHelpers;
using BenchmarkCassandraHelpers.Constants;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLockBase;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.LocksFactory;

namespace LockProcess
{
    public class LockProcessExe
    {
        public static void Main(string[] args)
        {
            new LockProcessExe().Run();
        }

        private void Run()
        {
            IProcessesCommunicator communicator = new ProcessesCommunicator(new Serializer(new AllPropertiesExtractor()));
            var processId = Guid.NewGuid().ToString();
            string lastLockId = null;
            communicator.AddRunningProcess(processId);
            while(true)
            {
                var signal = communicator.WaitStartSignal(processId, lastLockId);
                var lockId = signal.LockId;
                Console.WriteLine("Got signal");
                communicator.StartExecuting(lockId, processId);
                Run(communicator, signal, processId);
                communicator.StopExecuting(lockId, processId);
                lastLockId = lockId;
                Console.WriteLine("Success");
            }
        }

        private void Run(IProcessesCommunicator communicator, StartSignal signal, string processId)
        {
            var creator = GetCreator(communicator, signal);
            var results = new List<double>();
            Console.WriteLine();
            for(int i = 0; i < signal.LocksCount; i++)
            {
                var sw = new Stopwatch();
                sw.Start();
                using(creator.Lock(signal.LockId))
                {
                    Console.Write("\rGot {0}", i);
                    sw.Stop();
                    results.Add(sw.Elapsed.TotalMilliseconds);
                }
            }
            Console.WriteLine();
            communicator.WriteResults(signal.LockId, processId, results.ToArray());
        }

        private IRemoteLockCreator GetCreator(IProcessesCommunicator communicator, StartSignal signal)
        {
            if(signal.LockType == LockType.OldLock)
                return LocksCreatorFactory.CreateOldLock(communicator.GetCassandraCluster(), new ColumnFamilyFullName(OldLockConstants.Keyspace, OldLockConstants.ColumnFamily));
            if (signal.LockType == LockType.NewLockCassandraTTL)
                return LocksCreatorFactory.CreateNewLockWithCassandraTTL(communicator.GetCassandraCluster(), new ColumnFamilyFullName(NewWithCassandraTTLLockConstants.Keyspace, NewWithCassandraTTLLockConstants.ColumnFamily));
            throw new Exception(string.Format("Unknown lock type {0}", signal.LockType));
        }
    }
}