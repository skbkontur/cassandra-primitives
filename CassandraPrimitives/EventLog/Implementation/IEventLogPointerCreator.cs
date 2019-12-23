using SkbKontur.Cassandra.Primitives.EventLog.Primitives;

namespace SkbKontur.Cassandra.Primitives.EventLog.Implementation
{
    internal interface IEventLogPointerCreator
    {
        EventPointer Create(EventInfo eventInfo, string specificShard = null);
        EventPointer ToNextRow(string rowKey);
        string ChangeShard(string rowKey, string shard);
        string ChangeShard(long rowNumber, string shard);
        string GetShard(string rowKey);
        long GetRowNumber(EventInfo eventInfo);
    }
}