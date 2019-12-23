using SkbKontur.Cassandra.Primitives.EventLog.Primitives;

namespace SkbKontur.Cassandra.Primitives.EventLog.Implementation
{
    internal interface IEventLoggerAdditionalInfoRepository
    {
        EventInfo GetFirstEventInfo();
        EventInfo GetOrCreateFirstEventInfo(EventInfo eventInfo);

        void SetLastEventInfo(EventInfo eventInfo);
        EventInfo GetGoodLastEventInfo();
    }
}