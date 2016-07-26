using System;
using System.Collections.Generic;
using System.Net.NetworkInformation;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ChildProcessDriver;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.MainDriver;
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

            if (args.Length == 0)
                MainProcess();
            else
                ChildProcess(args);
        }

        private static void MainProcess()
        {
            var configuration = new TestConfiguration(
                amountOfThreads : 4,
                amountOfProcesses : 2,
                amountOfLocksPerThread : 20,
                minWaitTimeMilliseconds : 100,
                maxWaitTimeMilliseconds : 200,
                remoteHostName : IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName,
                httpPort : 12345,
                remoteLockImplementation : RemoteLockImplementations.Cassandra);

            var noDeploy = false;
            var teamCityLogger = new TeamCityLogger(Console.Out);
            var agentsProvider = new AgentProviderAllAgents();

            teamCityLogger.BeginMessageBlock("Cassandra deploy");
            var cassandraAgents = agentsProvider.AcquireAgents(1);
            new WrapperDeployer(teamCityLogger, noDeploy).DeployWrapperToAgents(cassandraAgents);
            var cassandraDriver = new CassandraMainDriver(teamCityLogger, cassandraAgents, noDeploy);

            using (cassandraDriver.StartCassandraCluster())
            {
                teamCityLogger.EndMessageBlock();
                var optionsSet = new Dictionary<string, object>
                    {
                        {"CassandraClusterSettings", cassandraDriver.ClusterSettings},
                        {"TestConfiguration", configuration},
                        {"LockId", Guid.NewGuid().ToString()}
                    };
                var driver = new MainDriver(teamCityLogger, configuration, agentsProvider, noDeploy);
                driver.Run(optionsSet);
            }
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