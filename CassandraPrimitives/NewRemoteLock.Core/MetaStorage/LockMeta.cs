namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.Core.MetaStorage
{
    public class LockMeta
    {
        public string LockId { get; set; }
        public int Index { get; set; }
        public int Count { get; set; }
        public string ColumnName { get; set; }
    }
}