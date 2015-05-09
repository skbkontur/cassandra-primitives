using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.TimeServiceCore.Settings
{
    public interface ITimeServiceSettings
    {
        TimeSpan ActualizeInterval { get; }
        bool NeedActualizeBeforeRequest { get; }
        string Keyspace { get; }
        string ColumnFamily { get; }

    }
}