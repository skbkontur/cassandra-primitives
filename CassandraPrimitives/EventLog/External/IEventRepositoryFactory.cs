using SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.External
{
    public interface IEventRepositoryFactory
    {
        IEventRepository CreateEventRepository(
            IShardCalculator shardCalculator,
            IEventRepositoryColumnFamilyFullNames columnFamilies);
    }
}