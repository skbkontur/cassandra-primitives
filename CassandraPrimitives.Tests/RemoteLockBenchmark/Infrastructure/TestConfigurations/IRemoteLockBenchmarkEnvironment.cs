namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations
{
    public interface IRemoteLockBenchmarkEnvironment
    {
        string AmountOfThreads { get; }
        string AmountOfProcesses { get; }
        string AmountOfLocksPerThread { get; }
        string MinWaitTimeMilliseconds { get; }
        string MaxWaitTimeMilliseconds { get; }
        string AmountOfClusterNodes { get; }
        string RemoteHostName { get; }
        string HttpPort { get; }
        string RemoteLockImplementation { get; }
        string TestScenario { get; }
    }
}