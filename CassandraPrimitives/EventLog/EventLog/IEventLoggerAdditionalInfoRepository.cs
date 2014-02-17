using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog
{
    internal interface IEventLoggerAdditionalInfoRepository
    {
        EventInfo GetFirstEventInfo();
        EventInfo GetOrCreateFirstEventInfo(EventInfo eventInfo);
        
        void SetLastEventInfo(EventInfo eventInfo);
        EventInfo GetGoodLastEventInfo();
    }
}