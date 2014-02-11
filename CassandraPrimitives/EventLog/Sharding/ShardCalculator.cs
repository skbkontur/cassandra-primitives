using System;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding
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