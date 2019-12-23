using NUnit.Framework;

namespace CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    public class BoxEventRepositoryMultiThread64ShardTest : BoxEventRepositoryMultiThreadTestBase
    {
        [Test, Ignore("Long running")]
        public void TestMultiple()
        {
            DoTestMultiple();
        }

        [Test]
        public void TestMultiThread()
        {
            DoTestMultiThread();
        }

        [Test]
        public void TestMultipleRepositoriesMultiThread()
        {
            DoTestMultipleRepositoriesMultiThread();
        }

        protected override int ShardsCount { get { return 64; } }
    }
}