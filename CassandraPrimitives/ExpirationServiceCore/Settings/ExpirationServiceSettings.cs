using System;

using SKBKontur.Catalogue.Core.Configuration.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Settings
{
    public class ExpirationServiceSettings : IExpirationServiceSettings
    {
        public ExpirationServiceSettings(IApplicationSettings settings)
        {
            ReadPeriod = settings.GetTimeSpan("ReadPeriod");
            ExpiryPeriod = settings.GetTimeSpan("ExpiryPeriod");
            Keyspace = settings.GetString("Keyspace");
            ColumnFamily = settings.GetString("ColumnFamily");
        }

        public TimeSpan ReadPeriod { get; set; }
        public TimeSpan ExpiryPeriod { get; set; }
        public string Keyspace { get; set; }
        public string ColumnFamily { get; set; }
    }
}