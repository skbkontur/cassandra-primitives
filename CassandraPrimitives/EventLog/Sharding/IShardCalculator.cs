using SkbKontur.Cassandra.Primitives.EventLog.Primitives;

namespace SkbKontur.Cassandra.Primitives.EventLog.Sharding
{
    public interface IShardCalculator
    {
        string CalculateShard(EventId eventId, object eventContent);
    }
}