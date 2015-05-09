using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService
{
    public class WeakRemoteLock : RemoteLockBase
    {
        public WeakRemoteLock(string lockId, ILockCreatorStorage lockCreatorStorage, IExpirationMonitoringStorage expirationMonitoringStorage, ITimeServiceClient timeServiceClient, RemoteLockSettings settings, out bool success)
            :
                base(lockId, lockCreatorStorage, expirationMonitoringStorage, timeServiceClient, settings)
        {
            if(!IsLockFree())
            {
                success = false;
                return;
            }
            AddThreadToQueue();
            while(true)
            {
                if(IsFirstInQueue())
                {
                    AddThreadToLock();
                    if(AmITheOnlyOwnerOfLock())
                    {
                        success = true;
                        break;
                    }
                    RemoveThreadFromLock();
                }
                else
                {
                    RemoveThreadFromQueue();
                    success = false;
                    break;
                }
            }
        }
    }
}