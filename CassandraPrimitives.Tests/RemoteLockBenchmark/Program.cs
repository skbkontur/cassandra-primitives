using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Logging;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    class Program
    {
        static void Main(string[] args)
        {
            Log4NetConfiguration.InitializeOnce();
            
            var teamCityLogger = new TeamCityLogger(Console.Out);
            teamCityLogger.BeginMessageBlock("Results");

            var configuration = new TestConfiguration
                {
                    amountOfThreads = 10,
                    amountOfLocksPerThread = 40,
                    maxWaitTimeMilliseconds = 100,
                };

            ITestResult testResult;
            using(var remoteLockGetter = new CassandraRemoteLockGetter())
            {
                var test = new SimpleTest(configuration, remoteLockGetter);
                test.Run();
                testResult = test.GetTestResult();
            }
                
            teamCityLogger.EndMessageBlock();

            var message = testResult.GetShortMessage();
            teamCityLogger.SetBuildStatus(TeamCityBuildStatus.Success, message);
        }
    }
}
