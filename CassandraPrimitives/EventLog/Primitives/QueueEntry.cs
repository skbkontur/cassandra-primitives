using System;
using System.Diagnostics;
using System.Threading.Tasks;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives
{
    internal class QueueEntry
    {
        public QueueEntry(TaskCompletionSource<ProcessResult> completionSource, EventStorageElement[] events, int priority)
        {
            this.completionSource = completionSource;
            this.events = events;
            this.priority = priority;
            sinceCreateStopwatch = Stopwatch.StartNew();
        }

        public TimeSpan SinceCreateElapsed { get { return sinceCreateStopwatch.Elapsed; } }

        public void Completed(ProcessResult result)
        {
            sinceResultSetStopwatch = Stopwatch.StartNew();
            completionSource.SetResult(result);
        }

        private readonly TaskCompletionSource<ProcessResult> completionSource;
        private readonly Stopwatch sinceCreateStopwatch;

        public readonly EventStorageElement[] events;
        public readonly int priority;

        public volatile Stopwatch sinceResultSetStopwatch;
    }
}