using GroboContainer.Core;
using GroboContainer.Impl;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.LockStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.MetaStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.QueueStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.RentExtender;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient;

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
            var metaStorage = new MetaStorage(cassandraCluster, serializer, remoteLockSettings);
            var lockStorage = new LockStorage(timeServiceClient, metaStorage, cassandraCluster, remoteLockSettings);
            var queueStorage = new QueueStorage(timeServiceClient, metaStorage, cassandraCluster, serializer, remoteLockSettings);
            var rentExtender = new RentExtender(queueStorage, lockStorage);
            return new NewRemoteLockCreator(new LockCreatorStorage(lockStorage, queueStorage, rentExtender), expirationMonitoringService, timeServiceClient, remoteLockSettings);
        }

        public static IRemoteLockCreator CreateNewLockWithCassandraTTL(ICassandraCluster cassandraCluster, ColumnFamilyFullName columnFamilyFullName)
        {
            var serializer = new Serializer(new AllPropertiesExtractor());
            var remoteLockSettings = new NewRemoteLock.WithCassanrdaTTL.RemoteLockSettings(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
            var metaStorage = new NewRemoteLock.WithCassanrdaTTL.MetaStorage.MetaStorage(cassandraCluster, serializer, remoteLockSettings);
            var queueStorage = new NewRemoteLock.WithCassanrdaTTL.QueueStorage.QueueStorage(metaStorage, cassandraCluster, serializer, remoteLockSettings);
            var lockStorage = new NewRemoteLock.WithCassanrdaTTL.LockStorage.LockStorage(metaStorage, cassandraCluster, remoteLockSettings);
            var rentExtender = new NewRemoteLock.WithCassanrdaTTL.RentExtender.RentExtender(queueStorage, lockStorage);
            var lockCreatorStorage = new NewRemoteLock.WithCassanrdaTTL.LockCreatorStorage.LockCreatorStorage(lockStorage, queueStorage, rentExtender);
            return new NewRemoteLock.WithCassanrdaTTL.NewRemoteLockCreator(lockCreatorStorage, remoteLockSettings);
        }
    }
}