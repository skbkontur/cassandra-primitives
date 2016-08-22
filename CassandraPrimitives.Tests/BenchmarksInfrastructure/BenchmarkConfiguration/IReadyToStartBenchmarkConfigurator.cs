using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Implementations.ZooKeeper.ZookeeperSettings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration
{
    public interface IReadyToStartBenchmarkConfigurator
    {
        IReadyToStartBenchmarkConfigurator WithCassandraCluster(ICassandraMetadataProvider cassandraMetadataProvider);
        IReadyToStartBenchmarkConfigurator WithExistingCassandraCluster(CassandraClusterSettings clusterSettings, ICassandraMetadataProvider cassandraMetadataProvider);
        IReadyToStartBenchmarkConfigurator WithZookeeperCluster();
        IReadyToStartBenchmarkConfigurator WithExistingZookeeperCluster(ZookeeperClusterSettings clusterSettings);
        IReadyToStartBenchmarkConfigurator WithClusterFromConfiguration(ICassandraMetadataProvider cassandraMetadataProvider);
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