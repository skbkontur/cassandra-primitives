using SkbKontur.Cassandra.Primitives.EventLog.Configuration.ColumnFamilies;
using SkbKontur.Cassandra.Primitives.EventLog.Profiling;
using SkbKontur.Cassandra.Primitives.EventLog.Sharding;

namespace SkbKontur.Cassandra.Primitives.EventLog
{
    public interface IEventRepositoryFactory
    {
        IEventRepository CreateEventRepository(
            IShardCalculator shardCalculator,
            IEventRepositoryColumnFamilyFullNames columnFamilies);

        IEventRepository CreateEventRepository(
            IShardCalculator shardCalculator,
            IEventRepositoryColumnFamilyFullNames columnFamilies,
            IEventLogProfiler profiler);
    }
}