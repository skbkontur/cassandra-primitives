﻿namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class RemoteLockCreator : IRemoteLockCreator
    {
        public RemoteLockCreator(IRemoteLockImplementation remoteLockImplementation)
        {
            this.remoteLockImplementation = remoteLockImplementation;
        }

        public IRemoteLock Lock(string lockId)
        {
            return new RemoteLock(remoteLockImplementation, lockId);
        }

        public bool TryGetLock(string lockId, out IRemoteLock remoteLock)
        {
            string concurrentThreadId;
            var weakRemoteLock = new WeakRemoteLock(remoteLockImplementation, lockId, out concurrentThreadId);
            remoteLock = weakRemoteLock;
            return string.IsNullOrEmpty(concurrentThreadId);
        }

        /// <summary>
        ///     Метод только для целей тестирования, им не нужно пользоваться. Поэтому его нет в интерфейсе
        /// </summary>
        /// <param name="lockId"></param>
        /// <returns></returns>
        public IRemoteLock LockWithoutLocalRivalOptimization(string lockId)
        {
            return new RemoteLock(remoteLockImplementation, lockId, localRivalOptimization : false);
        }

        /// <summary>
        ///     Метод только для целей тестирования, им не нужно пользоваться. Поэтому его нет в интерфейсе
        /// </summary>
        /// <param name="lockId"></param>
        /// <param name="remoteLock"></param>
        /// <returns></returns>
        public bool TryGetLockWithoutLocalRivalOptimization(string lockId, out IRemoteLock remoteLock)
        {
            string concurrentThreadId;
            var weakRemoteLock = new WeakRemoteLock(remoteLockImplementation, lockId, out concurrentThreadId, localRivalsOptimization : false);
            remoteLock = weakRemoteLock;
            return string.IsNullOrEmpty(concurrentThreadId);
        }

        private readonly IRemoteLockImplementation remoteLockImplementation;
    }
}