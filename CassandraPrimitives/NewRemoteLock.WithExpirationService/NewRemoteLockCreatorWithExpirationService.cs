using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLockBase;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService
{
    public class NewRemoteLockCreatorWithExpirationService : IRemoteLockCreator
    {
        public NewRemoteLockCreatorWithExpirationService(ILockCreatorStorage lockCreatorStorage, IExpirationMonitoringStorage expirationMonitoringStorage, ITimeGetter timeGetter, RemoteLockSettings settings)
        {
            this.lockCreatorStorage = lockCreatorStorage;
            this.expirationMonitoringStorage = expirationMonitoringStorage;
            this.timeGetter = timeGetter;
            this.settings = settings;
        }

        public IRemoteLock Lock(string lockId)
        {
            return new RemoteLock(lockId, lockCreatorStorage, expirationMonitoringStorage, timeGetter, settings);
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            bool success;
            var result = new WeakRemoteLock(lockId, lockCreatorStorage, expirationMonitoringStorage, timeGetter, settings, out success);
            remoteLock = success ? result : null;
            return success;
        }

        private readonly ILockCreatorStorage lockCreatorStorage;
        private readonly IExpirationMonitoringStorage expirationMonitoringStorage;
        private readonly ITimeGetter timeGetter;
        private readonly RemoteLockSettings settings;
    }
}