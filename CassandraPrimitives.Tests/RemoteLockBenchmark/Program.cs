using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class Program
    {
        private static void Main(string[] args)
        {
            var configuration = new TestConfiguration
                {
                    amountOfThreads = 5,
                    amountOfProcesses = 3,
                    amountOfLocksPerThread = 40,
                    maxWaitTimeMilliseconds = 100,
                    remoteHostName = Environment.MachineName + "." + Environment.UserDomainName
                };

            var driver = new MainDriver(new TeamCityLogger(Console.Out), configuration);
            driver.Run();
        }
    }
}