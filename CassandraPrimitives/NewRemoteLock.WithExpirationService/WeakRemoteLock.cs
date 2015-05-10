using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService
{
    public class WeakRemoteLock : RemoteLockBase
    {
        public WeakRemoteLock(string lockId, ILockCreatorStorage lockCreatorStorage, IExpirationMonitoringStorage expirationMonitoringStorage, ITimeGetter timeGetter, RemoteLockSettings settings, out bool success)
            :
                base(lockId, lockCreatorStorage, expirationMonitoringStorage, timeGetter, settings)
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