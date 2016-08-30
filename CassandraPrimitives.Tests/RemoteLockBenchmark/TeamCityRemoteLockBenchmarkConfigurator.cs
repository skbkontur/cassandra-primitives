using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Threading.Tasks;

using log4net;

using Metrics;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.CassandraInitialisation;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.BenchmarkConfiguration;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Implementations.Cassandra;
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

            teamCityLogger = new TeamCityLogger(Console.Out);
            this.staticRegistryCreatorMethod = staticRegistryCreatorMethod;
        }

        public void Run()
        {
            ClearArtifactsDirectories();

            InitMetrics();
            try
            {
                bool permissionToStart = false;
                var taskCompletionSource = new TaskCompletionSource<bool>();

                var variableProvider = new EnvironmentVariableProvider();

                ManyOptionsBenchmarkConfigurator
                    .CreateNew(variableProvider, staticRegistryCreatorMethod)
                    .WithAgentProviderFromTeamCity(variableProvider)
                    .WithTeamCityLogger(teamCityLogger)
                    //.WithClusterFromConfiguration(new CassandraMetaProvider())
                    //.WithJmxTrans(JmxGraphitePrefix)
                    .WithExistingCassandraCluster(
                        new CassandraClusterSettings(
                            "test_cluster",
                            new[]
                                {
                                    new IPEndPoint(IPAddress.Parse("10.33.63.133"), 9160),
                                    new IPEndPoint(IPAddress.Parse("10.33.61.141"), 9160),
                                    new IPEndPoint(IPAddress.Parse("10.33.62.136"), 9160)
                                },
                            new IPEndPoint(IPAddress.Parse("10.33.63.133"), 9160)),
                        new CassandraMetaProvider())
                    .WithJmxTrans(JmxGraphitePrefix, new[]
                        {
                            Tuple.Create("load01localcat.kontur", 7199),
                            Tuple.Create("load02localcat.kontur", 7199),
                            Tuple.Create("load03localcat.kontur", 7199),
                        })
                    .WithSetUpAction(() =>
                        {
                            permissionToStart = false;
                            taskCompletionSource = new TaskCompletionSource<bool>();
                        })
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
            Directory.CreateDirectory(artifactsDir);
        }

        private void CopyArtifacts()
        {
            try
            {
                if (Directory.Exists(metricsDir))
                {
                    ZipFile.CreateFromDirectory(metricsDir, Path.Combine(artifactsDir, "MetricsLogs.zip"), compressionLevel, false);
                }
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

        private const CompressionLevel compressionLevel = CompressionLevel.Optimal;

        private readonly ILog logger;
        private readonly TeamCityLogger teamCityLogger;
        private readonly Func<IScenariosRegistry> staticRegistryCreatorMethod;
        private readonly string metricsDir, logsDir, artifactsDir;
    }
}