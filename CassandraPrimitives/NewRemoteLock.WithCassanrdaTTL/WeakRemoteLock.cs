using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL
{
    public class WeakRemoteLock : RemoteLockBase
    {
        public WeakRemoteLock(string lockId, ILockCreatorStorage lockCreatorStorage, RemoteLockSettings settings, ITimeGetter timeGetter, out bool success)
            :
                base(lockId, lockCreatorStorage, timeGetter, settings)
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