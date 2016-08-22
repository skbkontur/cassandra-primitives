using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations
{
    public interface IRemoteLock
    {
        IDisposable Acquire();
        bool TryAcquire(out IDisposable remoteLock);
        void Release();
    }
}