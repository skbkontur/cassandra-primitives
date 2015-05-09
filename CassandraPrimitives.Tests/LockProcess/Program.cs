using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;

using BenchmarkCassandraHelpers;

using GroboContainer.Core;
using GroboContainer.Impl;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.LockStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.MetaStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.QueueStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.RentExtender;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient;

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
            var serializer = new Serializer(new AllPropertiesExtractor());
            var columnFamilyFullName = new ColumnFamilyFullName(processParameters.Keyspace, processParameters.ColumnFamily);
            if(processParameters.LockType == LockType.NewLock)
            {
                var container = new Container(new ContainerConfiguration(AssembliesLoader.Load()));
                var remoteLockSettings = new RemoteLockSettings(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
                var metaStorage = new MetaStorage(cassandraCluster, serializer, remoteLockSettings);
                var timeServiceClient = container.Get<ITimeServiceClient>();
                var lockStorage = new LockStorage(timeServiceClient, metaStorage, cassandraCluster, remoteLockSettings);
                var queueStorage = new QueueStorage(timeServiceClient, metaStorage, cassandraCluster, serializer, remoteLockSettings);
                var rentExtender = new RentExtender(queueStorage, lockStorage);
                var expirationMonitoringService = new ExpirationMonitoringStorage(cassandraCluster, serializer, ColumnFamilies.expirationMonitoring);
                return new NewRemoteLockCreator(new LockCreatorStorage(lockStorage, queueStorage, rentExtender), expirationMonitoringService, timeServiceClient, remoteLockSettings);
            }
            if(processParameters.LockType == LockType.OldLock)
                return new RemoteLockCreator(new CassandraRemoteLockImplementation(cassandraCluster, serializer, columnFamilyFullName));
            throw new Exception(string.Format("Unknown lock type {0}", processParameters.LockType));
        }
    }
}