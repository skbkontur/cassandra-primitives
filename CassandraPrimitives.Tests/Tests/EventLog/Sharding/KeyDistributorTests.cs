using System;
using System.Diagnostics;
using System.IO;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Sharding;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.Tests.EventLog.Sharding
{
    [TestFixture]
    public class KeyDistributorTests
    {
        [Test]
        public void TestGuidDistribution()
        {
            const int shardsCount = 10;
            var keyDistributor = KeyDistributor.Create(shardsCount);
            var array = new int[shardsCount];
            const int guidsCount = 10000;
            var sw = Stopwatch.StartNew();
            for (var i = 0; i < guidsCount; i++)
            {
                var idx = keyDistributor.Distribute(Guid.NewGuid().ToString());
                if (idx < 0 || idx >= shardsCount)
                    Assert.That(false);
                array[idx]++;
            }
            Console.WriteLine("Time=" + sw.Elapsed);
            var diff = array.Max() - array.Min();
            Console.WriteLine("Diff = " + diff);
            Assert.That(diff < guidsCount / 50);
        }

        [Test]
        public void TestRandomStringDistribution()
        {
            const int shardsCount = 10;
            var keyDistributor = KeyDistributor.Create(shardsCount);
            var array = new int[shardsCount];
            var path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Tests/EventLog/Sharding/Files/dict.txt");
            var words = File.ReadAllText(path).Split('\n', '\r').Where(s => !string.IsNullOrEmpty(s)).ToArray();
            var sw = Stopwatch.StartNew();
            for (var i = 0; i < words.Length; i++)
            {
                var idx = keyDistributor.Distribute(words[i]);
                if (idx < 0 || idx >= shardsCount)
                    Assert.That(false);
                array[idx]++;
            }
            Console.WriteLine("Time=" + sw.Elapsed);
            var diff = array.Max() - array.Min();
            Console.WriteLine("Diff = " + diff);
            Assert.That(diff < words.Length / 100);
        }
    }
}