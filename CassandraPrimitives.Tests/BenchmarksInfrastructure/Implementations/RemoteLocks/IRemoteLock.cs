using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.RemoteLocks
{
    public interface IRemoteLock
    {
        IDisposable Acquire();
        bool TryAcquire(out IDisposable remoteLock);
        void Release();
    }
}