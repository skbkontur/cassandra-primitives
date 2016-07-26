using System;
using System.Collections.Generic;
using System.Net.NetworkInformation;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Agents;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class Program
    {
        private static void Main(string[] args)
        {
            Log4NetConfiguration.InitializeOnce();
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
            var configuration = new TestConfiguration(
                amountOfThreads : 4,
                amountOfProcesses : 3,
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
            var cassandraAgents = agentsProvider.AcquireAgents(3);
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

        private static void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine(e.ExceptionObject.ToString());
            Environment.Exit(1);
        }
    }
}