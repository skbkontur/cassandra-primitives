using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives
{
    public class EventInfo : IComparable<EventInfo>
    {
        public int CompareTo(EventInfo other)
        {
            var ticksComparisonResult = Ticks.CompareTo(other.Ticks);
            if (ticksComparisonResult != 0) return ticksComparisonResult;
            return Id.CompareTo(other.Id);
        }

        public EventId Id { get; set; }
        public long Ticks { get; set; }
        public string Shard { get; set; }
    }
}