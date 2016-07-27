using System;
using System.Net.NetworkInformation;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests;
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

            var configuration = new TestConfiguration(
                amountOfThreads : 15,
                amountOfProcesses : 3,
                amountOfLocksPerThread : 400,
                minWaitTimeMilliseconds : 100,
                maxWaitTimeMilliseconds : 200,
                remoteHostName : IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName,
                httpPort : 12345,
                remoteLockImplementation : RemoteLockImplementations.Cassandra);

            var noDeploy = false;
            var teamCityLogger = new TeamCityLogger(Console.Out);
            var agentProvider = new AgentProviderAllAgents();

            BenchmarkConfigurator
                .Configure()
                .WithAgentProvider(agentProvider)
                .WithCassandraCluster(3)
                .WithTeamCityLogger(teamCityLogger)
                .WithConfiguration(configuration)
                .WithTest<TimelineTest>()
                .Start();
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