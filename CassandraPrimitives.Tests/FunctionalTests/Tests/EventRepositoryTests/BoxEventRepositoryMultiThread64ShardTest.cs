﻿using NUnit.Framework;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Tests.EventRepositoryTests
{
    public class BoxEventRepositoryMultiThread64ShardTest : BoxEventRepositoryMultiThreadTestBase
    {
        [Test, Ignore]
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