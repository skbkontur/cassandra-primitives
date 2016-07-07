using System;
using System.Threading;

using log4net;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.Logging;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class TestRunner : IDisposable
    {
        public TestRunner(TestConfiguration configuration, IExternalLogger externalLogger)
        {
            this.configuration = configuration;
            this.externalLogger = externalLogger;
            logger = LogManager.GetLogger(GetType());
        }

        public T RunTest<T>(ITest<T> test) where T : ITestResult
        {
            test.SetUp();

            var threads = new Thread[configuration.amountOfThreads];
            for (int i = 0; i < configuration.amountOfThreads; i++)
            {
                var threadInd = i;
                threads[i] = new Thread(() =>
                    {
                        try
                        {
                            test.DoWorkInSingleThread(threadInd);
                        }
                        catch (Exception e)
                        {
                            LogException("Exception occured in one of test threads", e);
                        }
                    });
            }

            try
            {
                foreach (var thread in threads)
                    thread.Start();
                foreach (var thread in threads)
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
            externalLogger.Log("{0}:\n{1}", description, exception);
        }

        public void Dispose()
        {
        }

        private readonly TestConfiguration configuration;
        private readonly IExternalLogger externalLogger;
        private readonly ILog logger;
    }
}