using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.CasRemoteLock
{
    public class CasRemoteLockerPreparedStatements
    {
        public CasRemoteLockerPreparedStatements(PreparedStatement tryProlongStatement, PreparedStatement tryAcquireStatement, PreparedStatement releaseStatement, PreparedStatement getLockOwnerStatement)
        {
            TryProlongStatement = tryProlongStatement;
            TryAcquireStatement = tryAcquireStatement;
            ReleaseStatement = releaseStatement;
            GetLockOwnerStatement = getLockOwnerStatement;
        }

        public PreparedStatement TryProlongStatement { get; private set; }
        public PreparedStatement TryAcquireStatement { get; private set; }
        public PreparedStatement ReleaseStatement { get; private set; }
        public PreparedStatement GetLockOwnerStatement {get; private set; }
    }
}