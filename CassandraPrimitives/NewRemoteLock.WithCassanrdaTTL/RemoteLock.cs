using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL
{
    public class RemoteLock : RemoteLockBase
    {
        public RemoteLock(string lockId, ILockCreatorStorage lockCreatorStorage, RemoteLockSettings settings)
            :
                base(lockId, lockCreatorStorage, settings)
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