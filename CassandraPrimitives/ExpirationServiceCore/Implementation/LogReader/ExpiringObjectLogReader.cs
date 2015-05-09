using System;

using SKBKontur.Catalogue.CassandraPrimitives.Storages.ExpirationMonitoringStorage;
using SKBKontur.Catalogue.CassandraPrimitives.TimeServiceClient;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Implementation.LogReader
{
    public class ExpiringObjectLogReader : IExpiringObjectLogReader
    {
        public ExpiringObjectLogReader(ITimeServiceClient timeServiceClient, IExpirationMonitoringStorage expirationMonitoringStorage)
        {
            this.timeServiceClient = timeServiceClient;
            this.expirationMonitoringStorage = expirationMonitoringStorage;
        }

        public ExpiringObjectMeta[] GetNewMetas()
        {
            var now = timeServiceClient.GetNowTicks();
            lastReadTime = lastReadTime ?? now - TimeSpan.FromDays(150).Ticks;
            var result = expirationMonitoringStorage.GetEntries(lastReadTime.Value - GetDiff(), now);
            lastReadTime = now;
            return result;
        }

        private long GetDiff()
        {
            return 0; // TODO latency
        }

        private long? lastReadTime;
        private readonly ITimeServiceClient timeServiceClient;
        private readonly IExpirationMonitoringStorage expirationMonitoringStorage;
    }
}