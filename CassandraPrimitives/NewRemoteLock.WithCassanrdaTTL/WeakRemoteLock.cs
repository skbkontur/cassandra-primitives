using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL
{
    public class WeakRemoteLock : RemoteLockBase
    {
        public WeakRemoteLock(string lockId, ILockCreatorStorage lockCreatorStorage, RemoteLockSettings settings, out bool success)
            :
                base(lockId, lockCreatorStorage, settings)
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