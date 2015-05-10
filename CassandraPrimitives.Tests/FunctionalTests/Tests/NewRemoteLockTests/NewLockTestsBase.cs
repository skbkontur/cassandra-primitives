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
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.NewRemoteLockTests
{
    public abstract class NewLockTestsBase
    {
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

        protected void ConfigureContainer(RemoteLockSettings settings)
        {
            var assemblies = AssembliesLoader.Load();
            container = new Container(new ContainerConfiguration(assemblies));
            container.Configurator.ForAbstraction<ISerializer>().UseInstances(new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead));
            container.Configurator.ForAbstraction<ICassandraClusterSettings>().UseInstances(cassandraClusterSettings);
            var storageFactory = container.Get<IExpirationMonitoringStorageFactory>();
            container.Configurator.ForAbstraction<IExpirationMonitoringStorage>().UseInstances(storageFactory.CreateStorage(ColumnFamilies.expirationMonitoring));
            container.Configurator.ForAbstraction<RemoteLockSettings>().UseInstances(settings);
        }

        protected ICassandraClusterSettings cassandraClusterSettings;
        protected Container container;

        private void StartServices()
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

        private void StopServices()
        {
            timeServiceProcess.Kill();
            expirationServiceProcess.Kill();
        }

        private Process timeServiceProcess;
        private Process expirationServiceProcess;
    }
}