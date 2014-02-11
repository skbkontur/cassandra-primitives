using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding
{
    public interface IShardCalculator
    {
        string CalculateShard(EventId eventId, object eventContent);
    }
}