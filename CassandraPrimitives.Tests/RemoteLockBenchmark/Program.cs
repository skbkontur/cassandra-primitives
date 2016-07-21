using System;
using System.Net.NetworkInformation;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkCommons.TestConfigurations;
using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class Program
    {
        private static void Main(string[] args)
        {
            /*var configuration = new TestConfiguration
                {
                    amountOfThreads = 15,
                    amountOfProcesses = 5,
                    amountOfLocksPerThread = 500,
                    maxWaitTimeMilliseconds = 200,
                    remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName,
                };*/
            var configuration = new TestConfiguration
                {
                    amountOfThreads = 10,
                    amountOfProcesses = 3,
                    amountOfLocksPerThread = 200,
                    minWaitTimeMilliseconds = 100,
                    maxWaitTimeMilliseconds = 300,
                    remoteHostName = IPGlobalProperties.GetIPGlobalProperties().HostName + "." + IPGlobalProperties.GetIPGlobalProperties().DomainName,
                };
            var driver = new MainDriver(new TeamCityLogger(Console.Out), configuration, true);
            driver.Run();
        }
    }
}