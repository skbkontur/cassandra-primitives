using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;

using log4net;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Scenarios.TestOptions;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.SeriesOfLocks;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Timeline;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.WaitForLock;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class TeamCityRemoteLockBenchmarkConfigurator
    {
        public TeamCityRemoteLockBenchmarkConfigurator(Func<IScenariosRegistry> staticRegistryCreatorMethod)
        {
            Log4NetConfiguration.InitializeOnce();
            logger = LogManager.GetLogger(typeof(TeamCityRemoteLockBenchmarkConfigurator));
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
            environment = RemoteLockBenchmarkEnvironment.GetFromEnvironment();
            teamCityLogger = new TeamCityLogger(Console.Out);
            this.staticRegistryCreatorMethod = staticRegistryCreatorMethod;
        }

        private void RunWithConfigurationAndOptions(TestConfiguration configuration, int configurationInd, ITestOptions options, int optionsInd)
        {
            teamCityLogger.BeginMessageBlock(string.Format("Test configuration - {0}, options set - {1}", configurationInd, optionsInd));
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Configuration:\n{0}", configuration);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Options:\n{0}", options);

            BenchmarkConfigurator
                .CreateNew()
                .WithStaticRegistryCreatorMethod(staticRegistryCreatorMethod)
                .WithAgentProviderFromTeamCity()
                .WithConfiguration(configuration)
                .WithTestOptions(options)
                .WithMetricsContext(Metric.Context(string.Format("Test configuration - {0}, options set - {1}", configurationInd, optionsInd)))
                .WithTeamCityLogger(teamCityLogger)
                .WithClusterFromConfiguration()
                .StartAndWaitForFinish();
        }

        private void RunWithConfiguration(TestConfiguration configuration, int configurationInd)
        {
            List<ITestOptions> testOptionsList;
            TestScenarios testScenario;
            if (!Enum.TryParse(configuration.TestScenario, out testScenario))
                throw new Exception(string.Format("Unknown scenario {0}", configuration.TestScenario));
            switch (testScenario)
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
                throw new Exception(string.Format("Unknown scenario {0}", configuration.TestScenario));
            }

            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Going to run with {0} variants of options", testOptionsList.Count);

            foreach (var indexedTestOptions in testOptionsList.Select((o, i) => new {Opt = o, Ind = i}))
            {
                try
                {
                    RunWithConfigurationAndOptions(configuration, configurationInd, indexedTestOptions.Opt, indexedTestOptions.Ind);
                }
                finally
                {
                    teamCityLogger.EndMessageBlock();
                }
            }
        }

        public void Run()
        {
            InitMetrics();
            var testConfigurations = TestConfiguration.ParseWithRanges(environment);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Going to run {0} test configuration(s)", testConfigurations.Count);

            try
            {
                foreach (var indexedTestConfiguration in testConfigurations.Select((c, i) => new {Ind = i, Conf = c}))
                    RunWithConfiguration(indexedTestConfiguration.Conf, indexedTestConfiguration.Ind);
            }
            finally
            {
                var logsDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs");
                if (Directory.Exists(logsDir))
                    teamCityLogger.PublishArtifact(logsDir);
            }

            teamCityLogger.SetBuildStatus(TeamCityBuildStatus.Success, "Done");
        }

        private void InitMetrics()
        {
            Metric.SetGlobalContextName(string.Format("EDI.Benchmarks.{0}.{1}", Process.GetCurrentProcess().ProcessName.Replace('.', '_'), Environment.MachineName.Replace('.', '_')));
            Metric.Config.WithHttpEndpoint("http://*:1234/").WithAllCounters();
            var graphiteUri = new Uri(string.Format("net.{0}://{1}:{2}", "tcp", "graphite-relay.skbkontur.ru", "2003"));
            Metric.Config.WithReporting(x => x
                                                 .WithGraphite(graphiteUri, TimeSpan.FromSeconds(5))
                                                 .WithCSVReports(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs", "csv"), TimeSpan.FromMinutes(1))
                                                 .WithTextFileReport(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs", "textlog.txt"), TimeSpan.FromMinutes(1)));
        }

        private void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            Console.WriteLine(e.ExceptionObject.ToString());
            logger.Error(e.ExceptionObject.ToString());
            Environment.Exit(1);
        }

        private readonly ILog logger;
        private readonly RemoteLockBenchmarkEnvironment environment;
        private readonly TeamCityLogger teamCityLogger;
        private readonly Func<IScenariosRegistry> staticRegistryCreatorMethod;
    }
}