using System;

using SkbKontur.Cassandra.Primitives.EventLog.Primitives;

namespace SkbKontur.Cassandra.Primitives.EventLog.Sharding
{
    public class ShardCalculator : IShardCalculator
    {
        public ShardCalculator(Func<EventId, object, string> calculate)
        {
            this.calculate = calculate;
        }

        public string CalculateShard(EventId eventId, object eventContent)
        {
            return calculate(eventId, eventContent);
        }

        private readonly Func<EventId, object, string> calculate;
    }
}