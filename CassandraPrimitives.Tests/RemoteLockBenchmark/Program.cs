using System;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Scenarios.Tests;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class Program
    {
        private static void Main(string[] args)
        {
            Log4NetConfiguration.InitializeOnce();
            logger = LogManager.GetLogger(typeof(Program));

            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;

            var noDeploy = false;

            BenchmarkConfigurator
                .Configure()
                .WithAgentProviderFromTeamCity()
                .WithCassandraCluster()
                .WithTeamCityLogger()
                .WithConfigurationFromTeamCity()
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