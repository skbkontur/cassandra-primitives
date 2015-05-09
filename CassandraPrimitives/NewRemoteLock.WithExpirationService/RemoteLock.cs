using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService
{
    public class RemoteLock : RemoteLockBase
    {
        public RemoteLock(string lockId, ILockCreatorStorage lockCreatorStorage, IExpirationMonitoringStorage expirationMonitoringStorage, ITimeServiceClient timeServiceClient, RemoteLockSettings settings)
            :
                base(lockId, lockCreatorStorage, expirationMonitoringStorage, timeServiceClient, settings)
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