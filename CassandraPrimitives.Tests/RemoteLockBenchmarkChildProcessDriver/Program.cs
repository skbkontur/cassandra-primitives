using System;
using System.IO;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmarkChildProcessDriver
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
            int threadInd;
            if (!int.TryParse(args[0], out threadInd))
                Console.WriteLine("Invalid argument");
            TestConfiguration configuration;
            using (var httpExternalDataProvider = new HttpExternalDataGetter(args[1]))
                configuration = httpExternalDataProvider.GetTestConfiguration().Result;
            ChildProcessDriver.RunSingleTest(threadInd, configuration, AppDomain.CurrentDomain.BaseDirectory);
        }

        private static void OnUnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            File.AppendAllText("C:\\errors.txt", e.ExceptionObject.ToString());
        }
    }
}