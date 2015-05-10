using System;
using System.Diagnostics;
using System.IO;
using System.Net;

using GroboContainer.Core;
using GroboContainer.Impl;

using GroBuf;
using GroBuf.DataMembersExtracters;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.NewRemoteLockTests
{
    [TestFixture(LockType.CassandraTTL)]
    [TestFixture(LockType.ExpirationService)]
    public abstract class NewLockTestsBase
    {
        protected NewLockTestsBase(LockType lockType)
        {
            this.lockType = lockType;
        }

        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            cassandraClusterSettings = StartSingleCassandraSetUp.Node.CreateSettings(IPAddress.Loopback);
            var initializerSettings = new CassandraInitializerSettings();
            var cassandraSchemeActualizer = new CassandraSchemeActualizer(new CassandraCluster(cassandraClusterSettings), new CassandraMetaProvider(), initializerSettings);
            cassandraSchemeActualizer.AddNewColumnFamilies();
            StartServices();
        }

        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            StopServices();
        }

        protected void ConfigureContainer(int maxRowLength = 1000, int extendRentPeriod = 5000)
        {
            if(lockType == LockType.CassandraTTL)
                ConfigureForCassandraTTL(maxRowLength, extendRentPeriod);
            else
                ConfigureForExpirationService(maxRowLength, extendRentPeriod);
        }

        protected ICassandraClusterSettings cassandraClusterSettings;
        protected Container container;

        private void ConfigureForCassandraTTL(int maxRowLength, int extendRentPeriod)
        {
            var assemblies = AssembliesLoader.Load();
            container = new Container(new ContainerConfiguration(assemblies));
            container.Configurator.ForAbstraction<ISerializer>().UseInstances(new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead));
            container.Configurator.ForAbstraction<ICassandraClusterSettings>().UseInstances(cassandraClusterSettings);
            var settings = new RemoteLockSettings(ColumnFamilies.newRemoteLock.KeyspaceName, ColumnFamilies.newRemoteLock.ColumnFamilyName, maxRowLength, extendRentPeriod : extendRentPeriod, useLocalOptimization : extendRentPeriod == 5000);
            container.Configurator.ForAbstraction<RemoteLockSettings>().UseInstances(settings);
            container.Configurator.ForAbstraction<IRemoteLockCreator>().UseType<NewRemoteLockCreator>();
        }

        private void ConfigureForExpirationService(int maxRowLength, int extendRentPeriod)
        {
            var assemblies = AssembliesLoader.Load();
            container = new Container(new ContainerConfiguration(assemblies));
            container.Configurator.ForAbstraction<ISerializer>().UseInstances(new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead));
            container.Configurator.ForAbstraction<ICassandraClusterSettings>().UseInstances(cassandraClusterSettings);
            var storageFactory = container.Get<IExpirationMonitoringStorageFactory>();
            container.Configurator.ForAbstraction<IExpirationMonitoringStorage>().UseInstances(storageFactory.CreateStorage(ColumnFamilies.expirationMonitoring));
            var settings = new NewRemoteLock.WithExpirationService.RemoteLockSettings(ColumnFamilies.newRemoteLock.KeyspaceName, ColumnFamilies.newRemoteLock.ColumnFamilyName, maxRowLength, extendRentPeriod : extendRentPeriod, useLocalOptimization : extendRentPeriod == 5000);
            container.Configurator.ForAbstraction<NewRemoteLock.WithExpirationService.RemoteLockSettings>().UseInstances(settings);
            container.Configurator.ForAbstraction<IRemoteLockCreator>().UseType<NewRemoteLock.WithExpirationService.NewRemoteLockCreator>();
        }

        private void StartServices()
        {
            if(lockType == LockType.ExpirationService)
            {
                timeServiceProcess = Process.Start(new ProcessStartInfo
                {
                    FileName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\TimeService\bin\Debug\Catalogue.CassandraPrimitives.Tests.TimeService.exe"),
                    RedirectStandardOutput = false,
                    UseShellExecute = true,
                    WindowStyle = ProcessWindowStyle.Normal,
                    CreateNoWindow = false,
                });
                expirationServiceProcess = Process.Start(new ProcessStartInfo
                {
                    FileName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\ExpirationService\bin\Debug\Catalogue.CassandraPrimitives.Tests.ExpirationService.exe"),
                    RedirectStandardOutput = false,
                    UseShellExecute = true,
                    WindowStyle = ProcessWindowStyle.Normal,
                    CreateNoWindow = false,
                });
            }
        }

        private void StopServices()
        {
            if(timeServiceProcess != null)
                timeServiceProcess.Kill();
            if(expirationServiceProcess != null)
                expirationServiceProcess.Kill();
        }

        private Process timeServiceProcess;
        private Process expirationServiceProcess;
        private readonly LockType lockType;
    }
}