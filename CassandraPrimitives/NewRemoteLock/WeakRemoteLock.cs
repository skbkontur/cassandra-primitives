using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.LockCreatorStorage;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock
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