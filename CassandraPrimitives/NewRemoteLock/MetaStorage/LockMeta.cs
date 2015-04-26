namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.MetaStorage
{
    public class LockMeta
    {
        public string LockId { get; set; }
        public int Index { get; set; }
        public int Count { get; set; }
        public string ColumnName { get; set; }
    }
}