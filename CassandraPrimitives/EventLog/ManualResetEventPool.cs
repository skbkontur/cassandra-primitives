using System;
using System.Collections.Generic;
using System.Threading;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog
{
    public class ManualResetEventPool : IDisposable
    {
        public ManualResetEventPool()
        {
            events = new Queue<ManualResetEvent>();
        }

        public void Dispose()
        {
            lock(eventsLock)
            {
                while(events.Count != 0)
                {
                    var manualResetEvent = events.Dequeue();
                    manualResetEvent.Dispose();
                }
            }
        }

        public void Release(ManualResetEvent manualResetEvent)
        {
            lock(eventsLock)
            {
                events.Enqueue(manualResetEvent);
            }
        }

        public ManualResetEvent Acquire()
        {
            lock(eventsLock)
            {
                return events.Count == 0 ? new ManualResetEvent(false) : events.Dequeue();
            }
        }

        private readonly object eventsLock = new object();
        private readonly Queue<ManualResetEvent> events;
    }
}