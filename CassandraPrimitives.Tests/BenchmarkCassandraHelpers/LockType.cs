namespace BenchmarkCassandraHelpers
{
    public enum LockType
    {
        OldLock,
        NewLockCassandraTTL,
        NewLockExpirationService,
    }
}