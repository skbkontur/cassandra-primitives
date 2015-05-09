using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.ExpirationServiceCore.Settings
{
    public interface IExpirationServiceSettings
    {
        TimeSpan ReadPeriod { get; set; }
        TimeSpan ExpiryPeriod { get; set; }
        string Keyspace { get; set; }
        string ColumnFamily { get; set; }
    }
}