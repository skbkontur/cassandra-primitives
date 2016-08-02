namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public interface IRemoteLockBenchmarkEnvironment
    {
        string AmountOfLocksPerThread { get; }
        string MinWaitTimeMilliseconds { get; }
        string MaxWaitTimeMilliseconds { get; }
    }
}