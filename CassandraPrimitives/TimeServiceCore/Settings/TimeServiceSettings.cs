using System;

using SKBKontur.Catalogue.Core.Configuration.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Settings
{
    public class TimeServiceSettings : ITimeServiceSettings
    {
        public TimeServiceSettings(IApplicationSettings applicationSettings)
        {
            ActualizeInterval = applicationSettings.GetTimeSpan("ActualizeInterval");
            bool needActualizeBeforeRequest;
            NeedActualizeBeforeRequest = applicationSettings.TryGetBool("NeedActualizeBeforeRequest", out needActualizeBeforeRequest) && needActualizeBeforeRequest;
            Keyspace = applicationSettings.GetString("Keyspace");
            ColumnFamily = applicationSettings.GetString("ColumnFamily");
        }

        public TimeSpan ActualizeInterval { get; private set; }
        public bool NeedActualizeBeforeRequest { get; private set; }
        public string Keyspace { get; private set; }
        public string ColumnFamily { get; private set; }
    }
}