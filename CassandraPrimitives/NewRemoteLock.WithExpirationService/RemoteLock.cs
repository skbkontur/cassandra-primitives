using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService
{
    public class RemoteLock : RemoteLockBase
    {
        public RemoteLock(string lockId, ILockCreatorStorage lockCreatorStorage, IExpirationMonitoringStorage expirationMonitoringStorage, ITimeGetter timeGetter, RemoteLockSettings settings)
            :
                base(lockId, lockCreatorStorage, expirationMonitoringStorage, timeGetter, settings)
        {
            AddThreadToQueue();
            while(true)
            {
                if(IsFirstInQueue() && IsLockFree())
                {
                    AddThreadToLock();
                    if(AmITheOnlyOwnerOfLock())
                        break;
                    RemoveThreadFromLock();
                }
                RemoveFromLocal();
            }
        }
    }
}