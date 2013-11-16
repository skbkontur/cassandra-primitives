using System;
using System.Threading;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives
{
    public class DeferredResult : IDisposable
    {
        public DeferredResult(ManualResetEventPool pool)
        {
            this.pool = pool;
            manualResetEvent = pool.Acquire();
            manualResetEvent.Reset();
        }

        public void Dispose()
        {
            pool.Release(manualResetEvent);
        }

        public void WaitFinished()
        {
            manualResetEvent.WaitOne();
        }

        public void Signal()
        {
            manualResetEvent.Set();
        }

        public volatile EventInfo[] successInfos;
        public volatile EventId[] failureIds;
        private readonly ManualResetEventPool pool;
        private readonly ManualResetEvent manualResetEvent;
    }
}