using GroboContainer.Core;
using GroboContainer.Impl;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.LockStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.MetaStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.QueueStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.RentExtender;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLockBase;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient;

using TimeGetter = SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.TimeGetter;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.LocksFactory
{
    public static class LocksCreatorFactory
    {
        public static IRemoteLockCreator CreateOldLock(ICassandraCluster cassandraCluster, ColumnFamilyFullName columnFamilyFullName)
        {
            var serializer = new Serializer(new AllPropertiesExtractor());
            var remoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, columnFamilyFullName);
            return new RemoteLockCreator(remoteLockImplementation);
        }

        public static IRemoteLockCreator CreateNewLockWithExpirationService(ICassandraCluster cassandraCluster, ColumnFamilyFullName columnFamilyFullName)
        {
            var container = new Container(new ContainerConfiguration(AssembliesLoader.Load()));
            var serializer = new Serializer(new AllPropertiesExtractor());
            container.Configurator.ForAbstraction<ISerializer>().UseInstances(serializer);
            var timeServiceClient = container.Get<ITimeServiceClient>();
            var expirationMonitoringService = new ExpirationMonitoringStorage(cassandraCluster, serializer, ColumnFamilies.expirationMonitoring);
            var remoteLockSettings = new RemoteLockSettings(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
            var timeGetter = new TimeGetter(timeServiceClient);
            var metaStorage = new MetaStorage(timeGetter, cassandraCluster, serializer, remoteLockSettings);
            var lockStorage = new LockStorage(timeGetter, metaStorage, cassandraCluster, remoteLockSettings);
            var queueStorage = new QueueStorage(timeGetter, metaStorage, cassandraCluster, serializer, remoteLockSettings);
            var rentExtender = new RentExtender(queueStorage, lockStorage);
            return new NewRemoteLockCreatorWithExpirationService(new LockCreatorStorage(lockStorage, queueStorage, rentExtender), expirationMonitoringService, timeGetter, remoteLockSettings);
        }

        public static IRemoteLockCreator CreateNewLockWithCassandraTTL(ICassandraCluster cassandraCluster, ColumnFamilyFullName columnFamilyFullName)
        {
            var container = new Container(new ContainerConfiguration(AssembliesLoader.Load()));
            var serializer = new Serializer(new AllPropertiesExtractor());
            container.Configurator.ForAbstraction<ISerializer>().UseInstances(serializer);
            var timeServiceClient = container.Get<ITimeServiceClient>();
            var simpleTimeGetter = new NewRemoteLock.WithCassanrdaTTL.TimeGetter();
            var remoteLockSettings = new RemoteLockSettings(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
            var metaStorage = new MetaStorage(simpleTimeGetter, cassandraCluster, serializer, remoteLockSettings);
            var queueStorage = new QueueStorage(simpleTimeGetter, metaStorage, cassandraCluster, serializer, remoteLockSettings);
            var lockStorage = new LockStorage(simpleTimeGetter, metaStorage, cassandraCluster, remoteLockSettings);
            var rentExtender = new RentExtender(queueStorage, lockStorage);
            var lockCreatorStorage = new LockCreatorStorage(lockStorage, queueStorage, rentExtender);
            return new NewRemoteLockCreatorWithCassandraTTL(lockCreatorStorage, new TimeGetter(timeServiceClient), remoteLockSettings);
        }
    }
}