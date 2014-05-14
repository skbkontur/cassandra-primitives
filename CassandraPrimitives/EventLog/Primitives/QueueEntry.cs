using System;
using System.Diagnostics;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives
{
    internal class QueueEntry
    {
        public QueueEntry()
        {
            sinceCreateStopwatch = Stopwatch.StartNew();
        }

        public TimeSpan SinceCreateElapsed { get { return sinceCreateStopwatch.Elapsed; } }
        public EventStorageElement[] events;
        public DeferredResult result;
        public int priority;
        private readonly Stopwatch sinceCreateStopwatch;
    }
}