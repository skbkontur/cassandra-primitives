using System;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;

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
                    maxWaitTimeMilliseconds = 100
                };

            if (args.Length == 0)
                MainDriver.RunMainDriver(configuration);
            else
            {
                int threadInd;
                if (!int.TryParse(args[0], out threadInd))
                    Console.WriteLine("Invalid argument");
                ChildProcessDriver.RunSingleTest(threadInd, configuration, AppDomain.CurrentDomain.BaseDirectory);
            }
        }
    }
}