using System;
using System.Linq;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ChildProcessDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestRunning;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.ProgressMessages;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.TestProgressProcessors;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests;
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

        private static void Main(string[] args)
        {
            new Program().Run(args);
        }

        private void Run(string[] args)
        {
            var scenariosRegistry = CreateRegistry();

            if (args.Length < 1 || args[0] != BenchmarkConfigurator.ConstantBenchmarkToken)
            {
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
                            .WithScenariosRegistry(scenariosRegistry)
                            .WithAgentProviderFromTeamCity()
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
                    }
                }
            }
            else
                ChildProcess(args.Skip(1).ToArray(), scenariosRegistry);
        }

        private IScenariosRegistry CreateRegistry()
        {
            var scenariosRegistry = new ScenariosRegistry();

            scenariosRegistry.Register<TimelineProgressMessage, TimelineTest, TimelineTestProgressProcessor>(
                TestScenarios.Timeline,
                options =>
                {
                    var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                    return new TimelineTest(options.Configuration, remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter, options.ProcessInd);
                },
                options => new TimelineTestProgressProcessor(options.Configuration, options.TeamCityLogger));

            scenariosRegistry.Register<WaitForLockProgressMessage, WaitForLockTest, WaitForLockTestProgressProcessor>(
                TestScenarios.WaitForLock,
                options =>
                {
                    var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                    return new WaitForLockTest(options.Configuration, remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter);
                },
                options => new WaitForLockTestProgressProcessor(options.Configuration, options.TeamCityLogger));

            scenariosRegistry.Register<SeriesOfLocksProgressMessage, SeriesOfLocksTest, SeriesOfLocksTestProgressProcessor>(
                TestScenarios.SeriesOfLocks,
                options =>
                {
                    var remoteLockGetterProvider = new RemoteLockGetterProvider(options.ExternalDataGetter, options.Configuration, options.ExternalProgressLogger);
                    return new SeriesOfLocksTest(options.Configuration, remoteLockGetterProvider, options.ExternalProgressLogger, options.ExternalDataGetter);
                },
                options => new SeriesOfLocksTestProgressProcessor(options.Configuration, options.TeamCityLogger));

            return scenariosRegistry;
        }

        private void ChildProcess(string[] args, IScenariosRegistry scenariosRegistry)
        {
            if (args.Length < 3)
                throw new Exception("Not enough arguments");

            int processInd;
            if (!int.TryParse(args[0], out processInd))
                throw new Exception(string.Format("Invalid process id {0}", args[0]));

            logger.InfoFormat("Process id is {0}", processInd);
            logger.InfoFormat("Remote http address is {0}", args[1]);

            var processToken = args[2];

            TestConfiguration configuration;
            using (var httpExternalDataGetter = new HttpExternalDataGetter(args[1], 12345))
                configuration = httpExternalDataGetter.GetTestConfiguration().Result;
            logger.InfoFormat("Configuration was received");

            ChildProcessDriver.RunSingleTest(configuration, processInd, processToken, scenariosRegistry);
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