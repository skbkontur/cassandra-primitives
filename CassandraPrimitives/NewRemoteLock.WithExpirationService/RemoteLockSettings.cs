namespace SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.WithExpirationService
{
    public class RemoteLockSettings
    {
        public RemoteLockSettings(string keyspace, string columnFamily, int maxRowLength = 1000, int extendRentPeriod = 5000, bool useLocalOptimization = true)
        {
            KeyspaceName = keyspace;
            ColumnFamilyName = columnFamily;
            MaxRowLength = maxRowLength;
            ExtendRentPeriod = extendRentPeriod;
            UseLocalOptimization = useLocalOptimization;
        }

        public string KeyspaceName { get; private set; }
        public string ColumnFamilyName { get; private set; }
        public int MaxRowLength { get; private set; }
        public int ExtendRentPeriod { get; private set; }
        public bool UseLocalOptimization { get; private set; }
    }
}