using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.RemoteLock
{
    public class PreparedStatements
    {
        public PreparedStatements(PreparedStatement writeThreadStatement, PreparedStatement deleteThreadStatement, PreparedStatement threadAliveStatement, PreparedStatement searchThreadsStatement, PreparedStatement writeLockMetadataStatement, PreparedStatement tryGetLockMetadataStatement)
        {
            WriteThreadStatement = writeThreadStatement;
            DeleteThreadStatement = deleteThreadStatement;
            ThreadAliveStatement = threadAliveStatement;
            SearchThreadsStatement = searchThreadsStatement;
            WriteLockMetadataStatement = writeLockMetadataStatement;
            TryGetLockMetadataStatement = tryGetLockMetadataStatement;
        }

        public PreparedStatement WriteThreadStatement { get; private set; }
        public PreparedStatement DeleteThreadStatement { get; private set; }
        public PreparedStatement ThreadAliveStatement { get; private set; }
        public PreparedStatement SearchThreadsStatement { get; private set; }
        public PreparedStatement WriteLockMetadataStatement { get; private set; }
        public PreparedStatement TryGetLockMetadataStatement { get; private set; }
    }
}