﻿using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.EventLog
{
    public interface IEventStorage
    {
        void Write(EventLogRecord[] events, long timestamp, int? ttl = null);
        void Delete(EventInfo[] eventInfos, long timestamp);
    }
}