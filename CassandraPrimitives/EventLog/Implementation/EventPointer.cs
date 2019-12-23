namespace SkbKontur.Cassandra.Primitives.EventLog.Implementation
{
    internal class EventPointer
    {
        public string RowKey { get; set; }
        public string ColumnName { get; set; }
    }
}