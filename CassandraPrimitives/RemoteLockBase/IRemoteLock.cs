using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLockBase
{
    public interface IRemoteLock : IDisposable
    {
        string LockId { get; }
        string ThreadId { get; }
    }
}