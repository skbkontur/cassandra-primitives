using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Configuration.ColumnFamilies;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog
{
    public interface IEventRepositoryFactory
    {
        IEventRepository CreateEventRepository(
            IShardCalculator shardCalculator,
            IEventRepositoryColumnFamilyFullNames columnFamilies);
    }
}