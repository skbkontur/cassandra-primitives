using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Implementation
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