using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

using BenchmarkCassandraHelpers;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Catalogue.CassandraPrimitives.RemoteLockBase;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.LocksFactory;

namespace LockProcess
{
    public class Program
    {
        public static void Main(string[] args)
        {
            new Program().Run(new ProcessParameters(args));
        }

        private void Run(ProcessParameters processParameters)
        {
            IProcessesCommunicator communicator = new ProcessesCommunicator(new Serializer(new AllPropertiesExtractor()));
            var processId = processParameters.ProcessId;
            communicator.AddRunningProcess(processId);
            communicator.WaitStartSignal(processParameters.Keyspace, processParameters.ColumnFamily);
            var remoteLockCreator = GetLockCreator(communicator, processParameters);
            var results = new List<double>();
            for(var i = 0; i < processParameters.LocksCount; i++)
            {
                var sw = new Stopwatch();
                sw.Start();
                using(remoteLockCreator.Lock(processParameters.LockId))
                {
                    sw.Stop();
                    Console.WriteLine("Executed {0}", i);
                    Thread.Sleep(20);
                }
                results.Add(sw.Elapsed.TotalMilliseconds);
                Thread.Sleep(20);
            }
            communicator.WriteResults(processParameters.Keyspace, processParameters.ColumnFamily, processId, results.ToArray());
            communicator.RemoveRunningProcess(processId);
        }

        private IRemoteLockCreator GetLockCreator(IProcessesCommunicator communicator, ProcessParameters processParameters)
        {
            var cassandraCluster = communicator.GetCassandraCluster();
            var columnFamilyFullName = new ColumnFamilyFullName(processParameters.Keyspace, processParameters.ColumnFamily);
            switch(processParameters.LockType)
            {
            case LockType.OldLock:
                return LocksCreatorFactory.CreateOldLock(cassandraCluster, columnFamilyFullName);
            case LockType.NewLockCassandraTTL:
                return LocksCreatorFactory.CreateNewLockWithCassandraTTL(cassandraCluster, columnFamilyFullName);
            case LockType.NewLockExpirationService:
                return LocksCreatorFactory.CreateNewLockWithExpirationService(cassandraCluster, columnFamilyFullName);
            default:
                throw new Exception(string.Format("Unknown lock type {0}", processParameters.LockType));
            }
        }
    }
}