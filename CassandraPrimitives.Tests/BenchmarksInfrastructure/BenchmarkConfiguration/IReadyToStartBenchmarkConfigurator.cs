using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.ZooKeeper.ZookeeperSettings;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration
{
    public interface IReadyToStartBenchmarkConfigurator
    {
        IReadyToStartBenchmarkConfigurator WithCassandraCluster();
        IReadyToStartBenchmarkConfigurator WithExistingCassandraCluster(CassandraClusterSettings clusterSettings);
        IReadyToStartBenchmarkConfigurator WithZookeeperCluster();
        IReadyToStartBenchmarkConfigurator WithExistingZookeeperCluster(ZookeeperClusterSettings clusterSettings);
        IReadyToStartBenchmarkConfigurator WithClusterFromConfiguration();
        IReadyToStartBenchmarkConfigurator WithDefaultTeamCityLogger();
        IReadyToStartBenchmarkConfigurator WithTeamCityLogger(ITeamCityLogger teamCityLogger);
        IReadyToStartBenchmarkConfigurator WithOption(string name, object value);
        IReadyToStartBenchmarkConfigurator WithDynamicOption(string name, Func<object> valueProvider);
        IReadyToStartBenchmarkConfigurator WithAllProcessStartedHandler(Action onAllProcessesStarted);
        IReadyToStartBenchmarkConfigurator WithJmxTrans(string graphitePrefix);
        IReadyToStartBenchmarkConfigurator WithJmxTrans(string graphitePrefix, Tuple<string, int>[] additionalJmxEndPoints);
        IReadyToStartBenchmarkConfigurator WithSetUpAction(Action action);
        IReadyToStartBenchmarkConfigurator WithTearDownAction(Action action);
        void StartAndWaitForFinish();
    }
}