using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.Remoting.Messaging;

using Newtonsoft.Json;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Logging;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class Program
    {
        static Process[] StartProcesses(TestConfiguration configuration, ITeamCityLogger teamCityLogger)
        {
            var processes = new Process[configuration.amountOfProcesses];
            for (int i = 0; i < configuration.amountOfProcesses; i++)
            {
                teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Starting process {0}...", i);
                    
                var startInfo = new ProcessStartInfo
                {
                    FileName = Assembly.GetExecutingAssembly().Location,
                    Arguments = String.Format("{0}", i),
                    UseShellExecute = false,
                    RedirectStandardOutput = true,
                };
                processes[i] = Process.Start(startInfo);
            }
            return processes;
        }
        
        static void RunMainDriver(TestConfiguration configuration)
        {
            Log4NetConfiguration.InitializeOnce();

            var teamCityLogger = new TeamCityLogger(Console.Out);
            teamCityLogger.BeginMessageBlock("Results");

            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Starting cassandra...");
            using (var cassandraServerStarter = new CassandraServerStarter())
            {
                try
                {
                    var processes = StartProcesses(configuration, teamCityLogger);

                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Waiting for processes to finish...");
                    foreach (var process in processes)
                        process.WaitForExit();

                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Processing results...");
                    var testResults = processes.Select((process, index) =>
                        {
                            var data = process.StandardOutput.ReadToEnd();
                            var testResult = JsonConvert.DeserializeObject<SimpleTestResult>(data);
                            teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Normal, "Process {0} finished with result: {1}", index, testResult.GetShortMessage());
                            return testResult;
                        });
                    var mergedTestResult = SimpleTestResult.Merge(testResults);

                    teamCityLogger.EndMessageBlock();

                    teamCityLogger.SetBuildStatus(TeamCityBuildStatus.Success, mergedTestResult.GetShortMessage());
                }
                catch (Exception e)
                {
                    teamCityLogger.WriteMessageFormat(TeamCityMessageSeverity.Failure, "Exception occured while working with child processes:\n{0}", e);
                    teamCityLogger.EndMessageBlock();
                    teamCityLogger.SetBuildStatus("Fail", "Fail because of unexpected exceptions");
                }
            }
        }

        static void RunSingleTest(int processInd, TestConfiguration configuration)
        {
            SimpleTestResult testResult;
            var settings = new CassandraClusterSettings();
            using (var remoteLockGetter = new CassandraRemoteLockGetter(settings, _ => { }))
            {
                var test = new SimpleTest(configuration, processInd, remoteLockGetter);
                using (var testRunner = new TestRunner(configuration, _ => { }))
                {
                    testResult = testRunner.RunTest(test);
                }
            }
            Console.Write(JsonConvert.SerializeObject(testResult));
        }

        static void Main(string[] args)
        {
            var configuration = new TestConfiguration
            {
                amountOfThreads = 5,
                amountOfProcesses = 3,
                amountOfLocksPerThread = 40,
                maxWaitTimeMilliseconds = 100,
            };

            if (args.Length == 0)
            {
                RunMainDriver(configuration);
            }
            else
            {
                int threadInd;
                if (!int.TryParse(args[0], out threadInd))
                    Console.WriteLine("Invalid argument");
                RunSingleTest(threadInd, configuration);
            }
        }
    }
}
