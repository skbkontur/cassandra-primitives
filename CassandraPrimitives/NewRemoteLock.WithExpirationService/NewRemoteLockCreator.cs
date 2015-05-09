using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService
{
    public class NewRemoteLockCreator : IRemoteLockCreator
    {
        public NewRemoteLockCreator(ILockCreatorStorage lockCreatorStorage, IExpirationMonitoringStorage expirationMonitoringStorage, ITimeServiceClient timeServiceClient, RemoteLockSettings settings)
        {
            this.lockCreatorStorage = lockCreatorStorage;
            this.expirationMonitoringStorage = expirationMonitoringStorage;
            this.timeServiceClient = timeServiceClient;
            this.settings = settings;
        }

        public IRemoteLock Lock(string lockId)
        {
            return new RemoteLock(lockId, lockCreatorStorage, expirationMonitoringStorage, timeServiceClient, settings);
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            bool success;
            var result = new WeakRemoteLock(lockId, lockCreatorStorage, expirationMonitoringStorage, timeServiceClient, settings, out success);
            remoteLock = success ? result : null;
            return success;
        }

        private readonly ILockCreatorStorage lockCreatorStorage;
        private readonly IExpirationMonitoringStorage expirationMonitoringStorage;
        private readonly ITimeServiceClient timeServiceClient;
        private readonly RemoteLockSettings settings;
    }
}