using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

using log4net;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure
{
    public class Program
    {
        public Program()
        {
            Log4NetConfiguration.InitializeOnce();
            logger = LogManager.GetLogger(typeof(Program));
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
        }

        public void Run(string[] args, Func<IScenariosRegistry> registryCreator)
        {
            InitMetrics();
            var testConfigurations = TestConfiguration.GetFromEnvironmentWithRanges(RemoteLockBenchmarkEnvironment.GetFromEnvironment());
            var teamCityLogger = new TeamCityLogger(Console.Out);
            teamCityLogger.FormatMessage("Going to run {0} test configuration(s)", testConfigurations.Count);

            foreach (var indexedTestConfiguration in testConfigurations.Select((c, i) => new {Ind = i, Conf = c}))
            {
                teamCityLogger.BeginMessageBlock(string.Format("Test configuration {0}", indexedTestConfiguration.Ind));
                try
                {
                    var configurator = BenchmarkConfigurator
                        .CreateNew()
                        .WithRegistryCreator(registryCreator)
                        .WithAgentProviderFromTeamCity()
                        .WithMetricsContext(Metric.Context(string.Format("Test configuration {0}", indexedTestConfiguration.Ind)))
                        .WithTeamCityLogger(teamCityLogger)
                        .WithConfiguration(indexedTestConfiguration.Conf);
                    switch (indexedTestConfiguration.Conf.RemoteLockImplementation)
                    {
                    case RemoteLockImplementations.Cassandra:
                        configurator.WithCassandraCluster();
                        break;
                    case RemoteLockImplementations.Zookeeper:
                        configurator.WithZookeeperCluster();
                        break;
                    default:
                        throw new Exception(string.Format("Type of cluster for {0} is unknown", indexedTestConfiguration.Conf.RemoteLockImplementation));
                    }
                    configurator.StartAndWaitForFinish();
                }
                finally
                {
                    teamCityLogger.EndMessageBlock();
                    var logsDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs");
                    if (Directory.Exists(logsDir))
                        teamCityLogger.PublishArtifact(logsDir);
                }
            }
        }

        private void InitMetrics()
        {
            Metric.SetGlobalContextName(string.Format("EDI.Benchmarks.{0}.{1}", Process.GetCurrentProcess().ProcessName.Replace('.', '_'), Environment.MachineName.Replace('.', '_')));
            Metric.Config.WithHttpEndpoint("http://*:1234/").WithAllCounters();
            var graphiteUri = new Uri(string.Format("net.{0}://{1}:{2}", "tcp", "graphite-relay.skbkontur.ru", "2003"));
            Metric.Config.WithReporting(x => x
                .WithGraphite(graphiteUri, TimeSpan.FromSeconds(5))
                .WithCSVReports(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs", "csv"), TimeSpan.FromMinutes(1))
                .WithTextFileReport(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs", "txt"), TimeSpan.FromMinutes(1)));
        }

        private void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine(e.ExceptionObject.ToString());
            logger.Error(e.ExceptionObject.ToString());
            Environment.Exit(1);
        }

        private readonly ILog logger;
    }
}