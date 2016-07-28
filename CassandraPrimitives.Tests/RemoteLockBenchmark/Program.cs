using System;
using System.Linq;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ChildProcessDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class Program
    {
        private static void Main(string[] args)
        {
            Log4NetConfiguration.InitializeOnce();
            logger = LogManager.GetLogger(typeof(Program));

            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;

            if (args.Length < 1 || args[0] != BenchmarkConfigurator.ConstantBenchmarkToken)
            {
                var testConfigurations = TestConfiguration.GetFromEnvironmentWithRanges();
                var teamCityLogger = new TeamCityLogger(Console.Out);
                teamCityLogger.FormatMessage("Going to run {0} test configuration(s)", testConfigurations.Count);

                foreach (var indexedTestConfiguration in testConfigurations.Select((c, i) => new {Ind = i, Conf = c}))
                {
                    teamCityLogger.BeginMessageBlock(string.Format("Test configuration {0}", indexedTestConfiguration.Ind));
                    try
                    {
                        var configurator = BenchmarkConfigurator
                            .Configure()
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
                        configurator.Start();
                    }
                    finally
                    {
                        teamCityLogger.EndMessageBlock();
                    }
                }
            }
            else
                ChildProcess(args.Skip(1).ToArray());
        }

        private static void ChildProcess(string[] args)
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

            ChildProcessDriver.RunSingleTest(configuration, processInd, processToken);
        }

        private static void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine(e.ExceptionObject.ToString());
            logger.Error(e.ExceptionObject.ToString());
            Environment.Exit(1);
        }

        private static ILog logger;
    }
}