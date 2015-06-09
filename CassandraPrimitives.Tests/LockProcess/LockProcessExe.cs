using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

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
            Console.WriteLine();
            var localSw = new Stopwatch();
            IRemoteLock remoteLock;
            for(int i = 0; i < signal.LocksCount; i++)
            {
                var currentId = communicator.GetLockId(signal.LockId, i);
                localSw.Restart();
                var result = creator.TryGetLock(currentId, out remoteLock);
                localSw.Stop();
                if(result)
                {
                    Console.WriteLine("Got {0}", i);
                    Thread.Sleep(100);
                    results.Add(localSw.Elapsed.TotalMilliseconds);
                    remoteLock.Dispose();
                    communicator.MarkLockIdAsDone(currentId, processId);
                }
                else
                {
                    Console.WriteLine("Can't {0}", i);
                    results.Add(double.MaxValue);
                    communicator.MarkLockIdAsDone(currentId, processId);
                }
            }
            sw.Stop();
            Console.WriteLine();
            communicator.WriteResults(string.Format("{0}", signal.LockId), processId, results.ToArray());
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