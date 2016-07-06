using System.Threading;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public class TestRunner : ITestRunner
    {
        private readonly TestConfiguration configuration;

        public TestRunner(TestConfiguration configuration)
        {
            this.configuration = configuration;
        }
        public T RunTest<T>(ITest<T> test) where T : ITestResult
        {
            test.SetUp();

            var threads = new Thread[configuration.amountOfThreads];
            for (int i = 0; i < configuration.amountOfThreads; i++)
            {
                var threadInd = i;
                threads[i] = new Thread(() => test.DoWorkInSingleThread(threadInd));
            }

            foreach (var thread in threads)
                thread.Start();
            foreach (var thread in threads)
                thread.Join();//TODO timeout?

            test.TearDown();

            return test.GetTestResult();
        }

        public void Dispose()
        {

        }
    }
}