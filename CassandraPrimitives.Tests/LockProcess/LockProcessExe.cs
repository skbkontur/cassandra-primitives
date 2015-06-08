using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

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
                Console.WriteLine("Locks count = {0}", signal.LocksCount);
                Console.WriteLine("Lock type = {0}", signal.LockType);
                Console.WriteLine("Processes count = {0}", signal.ProcessIds.Length);
                communicator.StartExecuting(lockId, processId);
                Run(communicator, signal, processId);
                communicator.StopExecuting(lockId, processId);
                lastLockId = lockId;
                Console.WriteLine("Success");
            }
        }

        private void Run(IProcessesCommunicator communicator, StartSignal signal, string processId)
        {
            var sw = new Stopwatch();
            sw.Start();
            var creator = GetCreator(communicator, signal);
            var results = new List<double>();
            var resultsLocal = new List<double>();
            Console.WriteLine();
            var localSw = new Stopwatch();
            for(int i = 0; i < signal.LocksCount; i++)
            {
                localSw.Restart();
                using(creator.Lock(signal.LockId))
                {
                    localSw.Stop();
                    results.Add(sw.Elapsed.TotalMilliseconds);
                    resultsLocal.Add(localSw.Elapsed.TotalMilliseconds);
                    Console.Write("\rGot {0}", i);
                }
            }
            sw.Stop();
            Console.WriteLine();
            communicator.WriteResults(string.Format("{0}_Aggregation", signal.LockId), processId, results.ToArray());
            communicator.WriteResults(string.Format("{0}_Concrete", signal.LockId), processId, resultsLocal.ToArray());
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