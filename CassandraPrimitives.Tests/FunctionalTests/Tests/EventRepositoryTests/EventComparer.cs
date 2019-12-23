using System;
using System.Collections.Generic;

using SkbKontur.Cassandra.Primitives.EventLog.Primitives;

namespace CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    public class EventComparer : IComparer<Event>
    {
        public int Compare(Event x, Event y)
        {
            var ei1 = x.EventInfo;
            var ei2 = y.EventInfo;
            if (ei1.Ticks.CompareTo(ei2.Ticks) != 0) return ei1.Ticks.CompareTo(ei2.Ticks);
            if (ei1.Id.CompareTo(ei2.Id) != 0) return ei1.Id.CompareTo(ei2.Id);
            return String.Compare(ei1.Shard, ei2.Shard, StringComparison.Ordinal);
        }
    }
}