using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using log4net;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons;
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
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;

            metricsDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs");
            logsDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "LogsDirectory");
            artifactsDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Artifacts");

            ClearLogsDirectories();
            Log4NetConfiguration.InitializeOnce();
            logger = LogManager.GetLogger(typeof(TeamCityRemoteLockBenchmarkConfigurator));

            environment = RemoteLockBenchmarkEnvironment.GetFromEnvironment();
            teamCityLogger = new TeamCityLogger(Console.Out);
            this.staticRegistryCreatorMethod = staticRegistryCreatorMethod;
        }

        private void RunWithConfigurationAndOptions(TestConfiguration configuration, int configurationInd, ITestOptions options, int optionsInd)
        {
            teamCityLogger.BeginMessageBlock(string.Format("Test configuration - {0}, options set - {1}", configurationInd, optionsInd));
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Configuration:\n{0}", configuration);
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Options:\n{0}", options);

            bool permissionToStart = false;
            var taskCompletionSource = new TaskCompletionSource<bool>();

            var currentArtifactsDir = new DirectoryInfo(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "CurrentArtifacts"));
            if (currentArtifactsDir.Exists)
                currentArtifactsDir.Delete(true);

            MetricsContext metricsContext = null;
            var metricsContextName = string.Format("Test configuration - {0}, options set - {1}", configurationInd, optionsInd);
            teamCityLogger.BeginActivity(string.Format("Configuration - {0}/{1}, options - {2}/{3}", configurationInd, amountOfConfigurations, optionsInd, amountOfOptionsSets));
            try
            {
                metricsContext = Metric.Context(metricsContextName);
                BenchmarkConfigurator
                    .CreateNew()
                    .WithStaticRegistryCreatorMethod(staticRegistryCreatorMethod)
                    .WithAgentProviderFromTeamCity()
                    .WithConfiguration(configuration)
                    .WithTestOptions(options)
                    .WithMetricsContext(metricsContext)
                    .WithTeamCityLogger(teamCityLogger)
                    .WithClusterFromConfiguration()
                    .WithJmxTrans(JmxGraphitePrefix)
                    .WithDynamicOption("permission_to_start", () => permissionToStart)
                    .WithDynamicOption("response_on_start", () => taskCompletionSource.Task.Result)
                    .WithAllProcessStartedHandler(() =>
                        {
                            Task.Run(() =>
                                {
                                    permissionToStart = true;
                                    Task.Delay(1000).Wait();
                                    taskCompletionSource.SetResult(true);
                                });
                        })
                    .StartAndWaitForFinish();
            }
            finally
            {
                teamCityLogger.EndActivity();
                if (metricsContext != null)
                {
                    Metric.ShutdownContext(metricsContextName);
                    metricsContext.Dispose();
                }
                if (currentArtifactsDir.Exists)
                {
                    var testArtifactsPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Artifacts", string.Format("Config_{0}_Options_{1}", configurationInd, optionsInd));
                    currentArtifactsDir.CopyTo(new DirectoryInfo(testArtifactsPath));
                }
            }
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
            amountOfOptionsSets = testOptionsList.Count;

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
            ClearArtifactsDirectories();

            InitMetrics();
            var testConfigurations = TestConfiguration.ParseWithRanges(environment);
            amountOfConfigurations = testConfigurations.Count;
            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Going to run {0} test configuration(s)", testConfigurations.Count);
            try
            {
                foreach (var indexedTestConfiguration in testConfigurations.Select((c, i) => new {Ind = i, Conf = c}))
                    RunWithConfiguration(indexedTestConfiguration.Conf, indexedTestConfiguration.Ind);
            }
            finally
            {
                CopyArtifacts();
            }

            teamCityLogger.SetBuildStatus(TeamCityBuildStatus.Success, "Done");
        }

        private void ClearLogsDirectories()
        {
            if (Directory.Exists(logsDir))
                Directory.Delete(logsDir, true);
        }

        private void ClearArtifactsDirectories()
        {
            if (Directory.Exists(metricsDir))
                Directory.Delete(metricsDir, true);
            if (Directory.Exists(artifactsDir))
                Directory.Delete(artifactsDir, true);
        }

        private void CopyArtifacts()
        {
            try
            {
                if (Directory.Exists(metricsDir))
                    new DirectoryInfo(metricsDir).CopyTo(new DirectoryInfo(Path.Combine(artifactsDir, "MetricsLogs")));
                if (Directory.Exists(logsDir))
                    new DirectoryInfo(logsDir).CopyTo(new DirectoryInfo(Path.Combine(artifactsDir, "MainProcessLogs")));
            }
            catch (Exception e)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Warning, "Exception while copying artifacts: {0}", e);
            }
        }

        private void InitMetrics()
        {
            Metric.SetGlobalContextName(MetricsGraphitePrefix);
            Metric.Config.WithHttpEndpoint("http://*:1234/").WithAllCounters();
            var graphiteUri = new Uri(string.Format("net.{0}://{1}:{2}", "tcp", "graphite-relay.skbkontur.ru", "2003"));
            Metric.Config.WithReporting(x => x
                                                 .WithGraphite(graphiteUri, TimeSpan.FromSeconds(5))
                                                 .WithCSVReports(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs", "csv"), TimeSpan.FromMinutes(1), ";")
                                                 .WithTextFileReport(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs", "textMetrics.txt"), TimeSpan.FromMinutes(1)));
        }

        private string MetricsGraphitePrefix { get { return string.Format("EDI.Benchmarks.{0}.Metrics", Environment.MachineName.Replace('.', '_')); } }
        private string JmxGraphitePrefix { get { return string.Format("EDI.Benchmarks.{0}.Jmx", Environment.MachineName.Replace('.', '_')); } }

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
        private readonly string metricsDir, logsDir, artifactsDir;
        private int amountOfConfigurations, amountOfOptionsSets;
    }
}