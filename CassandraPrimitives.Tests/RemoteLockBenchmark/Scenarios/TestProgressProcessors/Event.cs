using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors
{
    public class Event : IComparable<Event>
    {
        public Event(long time, int type)
        {
            Time = time;
            Type = type;
        }

        public long Time { get; private set; }
        public int Type { get; private set; }

        public int CompareTo(Event other)
        {
            return Time == other.Time ? Type.CompareTo(other.Type) : Time.CompareTo(other.Time);
        }
    }
}