using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging.TestProgressProcessors
{
    public class Event : IComparable<Event>
    {
        public long Time { get; private set; }
        public int Type { get; private set; }

        public Event(long time, int type)
        {
            Time = time;
            Type = type;
        }

        public int CompareTo(Event other)
        {
            return Time == other.Time ? Type.CompareTo(other.Type) : Time.CompareTo(other.Time);
        }
    }
}