using System;
using System.Diagnostics;
using System.Threading;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Profiling;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Utils;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives
{
    internal class DeferredResult : IDisposable
    {
        public DeferredResult(ManualResetEventPool pool, IEventLogProfiler profiler)
        {
            this.pool = pool;
            this.profiler = profiler;
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
            if(signalAndWaitOneDiff != null)
                profiler.AfterDeferredResultWaitFinished(signalAndWaitOneDiff.Elapsed);
        }

        public void Signal()
        {
            signalAndWaitOneDiff = Stopwatch.StartNew();
            manualResetEvent.Set();
        }

        public volatile EventInfo[] successInfos;
        public volatile EventId[] failureIds;
        private Stopwatch signalAndWaitOneDiff;
        private readonly ManualResetEventPool pool;
        private readonly IEventLogProfiler profiler;
        private readonly ManualResetEvent manualResetEvent;
    }
}