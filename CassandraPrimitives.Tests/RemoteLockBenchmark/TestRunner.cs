using System;
using System.Diagnostics;
using System.Threading;

using log4net;

using SKBKontur.Catalogue.TeamCity;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class TestRunner : ITestRunner
    {
        private readonly TestConfiguration configuration;
        private readonly Action<string> externalLogger;
        private readonly ILog logger;

        public TestRunner(TestConfiguration configuration, Action<string> externalLogger)
        {
            this.configuration = configuration;
            this.externalLogger = externalLogger;
            logger = LogManager.GetLogger(GetType());
        }

        public T RunTest<T>(ITest<T> test) where T : ITestResult
        {
            test.SetUp();

            var threads = new Thread[configuration.amountOfThreads];
            for(int i = 0; i < configuration.amountOfThreads; i++)
            {
                var threadInd = i;
                threads[i] = new Thread(() =>
                    {
                        try
                        {
                            test.DoWorkInSingleThread(threadInd);
                        }
                        catch(Exception e)
                        {
                            LogException("Exception occured in one of test threads", e);
                        }
                    });
            }

            try
            {
                foreach(var thread in threads)
                    thread.Start();
                foreach(var thread in threads)
                    thread.Join(); //TODO timeout?
            }
            catch (Exception e)
            {
                LogException("Exception occured while perfoming test", e);
            }

            test.TearDown();

            return test.GetTestResult();
        }

        private void LogException(string description, Exception exception)
        {
            logger.Error(String.Format("{0}:", description), exception);
            externalLogger(String.Format("{0}:\n{1}", description, exception));
        }

        public void Dispose()
        {

        }
    }
}