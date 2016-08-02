using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

using log4net;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ChildProcessDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.SeriesOfLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Timeline;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.WaitForLock;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class Program
    {
        public Program()
        {
            Log4NetConfiguration.InitializeOnce();
            logger = LogManager.GetLogger(typeof(Program));
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
        }

        public static IScenariosRegistry CreateRegistry()
        {
            var scenariosRegistry = new ScenariosRegistry();

            scenariosRegistry.Register<TimelineProgressMessage, TimelineTest, TimelineTestProgressProcessor, TimelineTestOptions>(
                TestScenarios.Timeline,
                options =>
                    {
                        var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                        return new TimelineTest(remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter, options.ProcessInd);
                    },
                options => new TimelineTestProgressProcessor(options.Configuration, options.TestOptions as TimelineTestOptions, options.TeamCityLogger, options.MetricsContext));

            scenariosRegistry.Register<WaitForLockProgressMessage, WaitForLockTest, WaitForLockTestProgressProcessor, WaitForLockTestOptions>(
                TestScenarios.WaitForLock,
                options =>
                    {
                        var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                        return new WaitForLockTest(remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter);
                    },
                options => new WaitForLockTestProgressProcessor(options.Configuration, options.TestOptions as WaitForLockTestOptions, options.TeamCityLogger, options.MetricsContext));

            scenariosRegistry.Register<SeriesOfLocksProgressMessage, SeriesOfLocksTest, SeriesOfLocksTestProgressProcessor, SeriesOfLocksTestOptions>(
                TestScenarios.SeriesOfLocks,
                options =>
                    {
                        var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                        return new SeriesOfLocksTest(remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter);
                    },
                options => new SeriesOfLocksTestProgressProcessor(options.Configuration, options.TestOptions as SeriesOfLocksTestOptions, options.TeamCityLogger, options.MetricsContext));

            return scenariosRegistry;
        }

        private static void Main(string[] args)
        {
            new Program().Run();
        }

        private void Run()
        {
            InitMetrics();
            var environment = RemoteLockBenchmarkEnvironment.GetFromEnvironment();
            var testConfigurations = TestConfiguration.ParseWithRanges(environment);
            var teamCityLogger = new TeamCityLogger(Console.Out);
            teamCityLogger.FormatMessage("Going to run {0} test configuration(s)", testConfigurations.Count);

            foreach (var indexedTestConfiguration in testConfigurations.Select((c, i) => new {Ind = i, Conf = c}))
            {
                teamCityLogger.BeginMessageBlock(string.Format("Test configuration {0}", indexedTestConfiguration.Ind));
                try
                {
                    List<ITestOptions> testOptionsList;
                    switch (indexedTestConfiguration.Conf.TestScenario)
                    {
                    case TestScenarios.Timeline:
                        testOptionsList = TimelineTestOptions.ParseWithRanges(environment).Cast<ITestOptions>().ToList();
                        break;
                    case TestScenarios.WaitForLock:
                        testOptionsList = WaitForLockTestOptions.ParseWithRanges(environment).Cast<ITestOptions>().ToList();
                        break;
                    case TestScenarios.SeriesOfLocks:
                        testOptionsList = SeriesOfLocksTestOptions.ParseWithRanges(environment).Cast<ITestOptions>().ToList();
                        break;
                    default:
                        throw new Exception(string.Format("Unknown scenario {0}", indexedTestConfiguration.Conf.TestScenario));
                    }

                    foreach (var indexedTestOptions in testOptionsList.Select((o, i) => new {Opt = o, Ind = i}))
                    {
                        var configurator = BenchmarkConfigurator
                            .CreateNew()
                            .WithRegistryCreator(CreateRegistry)
                            .WithAgentProviderFromTeamCity()
                            .WithConfiguration(indexedTestConfiguration.Conf)
                            .WithTestOptions(indexedTestOptions.Opt)
                            .WithMetricsContext(Metric.Context(string.Format("Test configuration {0}, test option {1}", indexedTestConfiguration.Ind, indexedTestOptions.Ind)))
                            .WithTeamCityLogger(teamCityLogger);
                        switch (indexedTestConfiguration.Conf.ClusterType)
                        {
                        case ClusterTypes.Cassandra:
                            configurator.WithCassandraCluster();
                            break;
                        case ClusterTypes.Zookeeper:
                            configurator.WithZookeeperCluster();
                            break;
                        default:
                            throw new Exception(string.Format("Type of cluster for {0} is unknown", indexedTestConfiguration.Conf.ClusterType));
                        }
                        configurator.StartAndWaitForFinish();
                    }
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