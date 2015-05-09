using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;

namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithCassanrdaTTL
{
    public class NewRemoteLockCreator : IRemoteLockCreator
    {
        public NewRemoteLockCreator(ILockCreatorStorage lockCreatorStorage, RemoteLockSettings settings)
        {
            this.lockCreatorStorage = lockCreatorStorage;
            this.settings = settings;
        }

        public IRemoteLock Lock(string lockId)
        {
            return new RemoteLock(lockId, lockCreatorStorage, settings);
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            bool success;
            var result = new WeakRemoteLock(lockId, lockCreatorStorage, settings, out success);
            remoteLock = success ? result : null;
            return success;
        }

        private readonly ILockCreatorStorage lockCreatorStorage;
        private readonly RemoteLockSettings settings;
    }
}