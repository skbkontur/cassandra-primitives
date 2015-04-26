using System.Net;

using GroboContainer.Core;
using GroboContainer.Impl;

using GroBuf;
using GroBuf.DataMembersExtracters;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock;
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
        }

        protected void ConfigureContainer(RemoteLockSettings settings)
        {
            var assemblies = AssembliesLoader.Load();
            container = new Container(new ContainerConfiguration(assemblies));
            container.Configurator.ForAbstraction<ISerializer>().UseInstances(new Serializer(new AllPropertiesExtractor(), null, GroBufOptions.MergeOnRead));
            container.Configurator.ForAbstraction<ICassandraClusterSettings>().UseInstances(cassandraClusterSettings);
            container.Configurator.ForAbstraction<RemoteLockSettings>().UseInstances(settings);
        }

        protected ICassandraClusterSettings cassandraClusterSettings;
        protected Container container;
    }
}