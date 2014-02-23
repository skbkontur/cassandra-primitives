﻿using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

using GroBuf;
using GroBuf.DataMembersExtracters;

using GroboContainer.Core;
using GroboContainer.Impl;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Helpers;
using SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.SchemeActualizer;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;

using log4net;

using SKBKontur.Cassandra.ClusterDeployment;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Tests.RemoteLockTests
{
    [TestFixture]
    public abstract class RemoteLockTestBase
    {
        [TestFixtureSetUp]
        public virtual void TestFixtureSetUp()
        {
            var cassandraClusterSettings = StartSingleCassandraSetUp.Node.CreateSettings(IPAddress.Loopback);
            var initializerSettings = new CassandraInitializerSettings();
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(new CassandraCluster(cassandraClusterSettings), new CassandraMetaProvider(), initializerSettings);
            cassandraSchemeActualizer.AddNewColumnFamilies();
        }

        [SetUp]
        public virtual void SetUp()
        {
            Log4NetConfiguration.InitializeOnce();
            var assemblies = AssembliesLoader.Load();
            container = new Container(new ContainerConfiguration(assemblies));
            container.Configurator.ForAbstraction<ISerializer>().UseInstances(new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead));
            ConfigureContainer(container);
            var cassandraClusterSettings = StartSingleCassandraSetUp.Node.CreateSettings(IPAddress.Loopback);
            container.Configurator.ForAbstraction<ICassandraClusterSettings>().UseInstances(cassandraClusterSettings);
            var remoteLockImplementation = container.Create<ColumnFamilyFullName, CassandraRemoteLockImplementation>(ColumnFamilies.remoteLock);
            container.Configurator.ForAbstraction<IRemoteLockImplementation>().UseInstances(remoteLockImplementation);

            logger.InfoFormat("Start SetUp, runningThreads = {0}", runningThreads);
            runningThreads = 0;
            isEnd = false;
            threads = new List<Thread>();
        }

        [TearDown]
        public virtual void TearDown()
        {
            logger.InfoFormat("Start TeadDown, runningThreads = {0}", runningThreads);
            foreach(var thread in threads ?? new List<Thread>())
                thread.Abort();
        }

        protected virtual void ConfigureContainer(IContainer c)
        {
        }

        protected void AddThread(Action<Random> shortAction)
        {
            var seed = Guid.NewGuid().GetHashCode();
            var thread = new Thread(() => MakePeriodicAction(shortAction, seed));
            thread.Start();
            logger.InfoFormat("Add thread with seed = {0}", seed);
            threads.Add(thread);
        }

        protected void JoinThreads()
        {
            logger.Info("JoinThreads. begin");
            isEnd = true;
            running.Set();
            foreach(var thread in threads)
                Assert.That(thread.Join(TimeSpan.FromSeconds(180)), "Не удалось остановить поток");
            logger.Info("JoinThreads. end");
        }

        protected void RunThreads(int timeInterval = 1000)
        {
            logger.InfoFormat("RunThreads. begin, runningThreads = {0}", runningThreads);
            running.Set();
            Thread.Sleep(timeInterval);
            running.Reset();
            while(Interlocked.CompareExchange(ref runningThreads, 0, 0) != 0)
            {
                Thread.Sleep(50);
                logger.InfoFormat("Wait runningThreads = 0. Now runningThreads = {0}", runningThreads);
                foreach(var thread in threads)
                {
                    if(!thread.IsAlive)
                        throw new Exception("Поток сдох");
                }
            }
            logger.Info("RunThreads. end");
        }

        protected Container container;

        private void MakePeriodicAction(Action<Random> shortAction, int seed)
        {
            try
            {
                var localRandom = new Random(seed);
                while(!isEnd)
                {
                    running.WaitOne();
                    Interlocked.Increment(ref runningThreads);
                    shortAction(localRandom);
                    Interlocked.Decrement(ref runningThreads);
                }
            }
            catch(Exception e)
            {
                logger.Error(e);
            }
        }

        private readonly ManualResetEvent running = new ManualResetEvent(false);
        private int runningThreads;
        private volatile bool isEnd;

        private static readonly ILog logger = LogManager.GetLogger(typeof(RemoteLockTestBase));
        private List<Thread> threads;
    }
}