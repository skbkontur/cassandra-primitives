using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.Tests.EventLog.Linq
{
    [TestFixture]
    public class SortedMergeExternalTests
    {
        [Test]
        public void ManyMerge()
        {
            IEnumerable<int> z = new int[0];
            var lists = new int[20].Select((x, i) => (IEnumerable<int>)new List<int>(new[] {i}));
            var res = lists.Aggregate(z, (current, x) => current.SortedMerge(x)).ToArray();
            Assert.AreEqual(20, res.Length);
            for (var i = 0; i < 20; i++)
                Assert.AreEqual(i, res[i]);
        }

        [Test]
        public void Test12()
        {
            var a = new[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            var b = new[] {11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
            var res = a.SortedMerge(b).ToArray();
            Assert.AreEqual(20, res.Length);
            for (var i = 0; i < 20; i++)
                Assert.AreEqual(i + 1, res[i]);
        }

        [Test]
        public void Test21()
        {
            var a = new[] {11, 12, 13, 14, 15, 16, 17, 18, 19, 20};
            var b = new[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
            var res = a.SortedMerge(b).ToArray();
            Assert.AreEqual(20, res.Length);
            for (var i = 0; i < 20; i++)
                Assert.AreEqual(i + 1, res[i]);
        }

        [Test]
        public void Test1212()
        {
            var a = new[] {1, 3, 5, 7, 9, 11, 13, 15, 17, 19};
            var b = new[] {2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
            var res = a.SortedMerge(b).ToArray();
            Assert.AreEqual(20, res.Length);
            for (var i = 0; i < 20; i++)
                Assert.AreEqual(i + 1, res[i]);
        }

        [Test]
        public void Test2121()
        {
            var a = new[] {2, 4, 6, 8, 10, 12, 14, 16, 18, 20};
            var b = new[] {1, 3, 5, 7, 9, 11, 13, 15, 17, 19};
            var res = a.SortedMerge(b).ToArray();
            Assert.AreEqual(20, res.Length);
            for (var i = 0; i < 20; i++)
                Assert.AreEqual(i + 1, res[i]);
        }

        [Test]
        public void Test1()
        {
            var a = new[] {1, 2, 3};
            var b = new int[0];
            var res = a.SortedMerge(b).ToArray();
            Assert.AreEqual(3, res.Length);
            for (var i = 0; i < 3; i++)
                Assert.AreEqual(i + 1, res[i]);
        }

        [Test]
        public void Test2()
        {
            var a = new int[0];
            var b = new[] {1, 2, 3};
            var res = a.SortedMerge(b).ToArray();
            Assert.AreEqual(3, res.Length);
            for (var i = 0; i < 3; i++)
                Assert.AreEqual(i + 1, res[i]);
        }

        [Test]
        public void Test()
        {
            var a = new int[0];
            var b = new int[0];

            var res = a.SortedMerge(b).ToArray();
            Assert.AreEqual(0, res.Length);

            res = ((IEnumerable<int>)null).SortedMerge(null).ToArray();
            Assert.AreEqual(0, res.Length);
        }

        [Test]
        public void StressTestRandom()
        {
            const int arrLen = 20;
            var random = new Random();
            for (var testIteration = 0; testIteration < 10000; testIteration++)
            {
                var aSize = random.Next(arrLen);
                var bSize = arrLen - aSize;
                var a = new List<int>();
                var b = new List<int>();
                for (var i = 1; i <= arrLen; i++)
                {
                    var idx = GetZeroOrOne(random, aSize - a.Count, bSize - b.Count);
                    if (idx == 0)
                        a.Add(i);
                    else
                        b.Add(i);
                }
                var res = a.SortedMerge(b).ToArray();
                try
                {
                    Assert.AreEqual(arrLen, res.Length);
                    for (var i = 0; i < arrLen; i++)
                        Assert.AreEqual(i + 1, res[i]);
                }
                catch (Exception)
                {
                    Console.Write("a=");
                    for (var i = 0; i < a.Count; i++)
                        Console.Write(a[i] + ",");
                    Console.WriteLine();

                    Console.Write("b=");
                    for (var i = 0; i < b.Count; i++)
                        Console.Write(b[i] + ",");
                    Console.WriteLine();

                    Console.Write("res=");
                    for (var i = 0; i < res.Length; i++)
                        Console.Write(res[i] + ",");
                    Console.WriteLine();
                    throw;
                }
            }
        }

        private int GetZeroOrOne(Random random, int zeroP, int oneP)
        {
            var x = random.Next(zeroP + oneP);
            if (x < zeroP) return 0;
            return 1;
        }
    }
}