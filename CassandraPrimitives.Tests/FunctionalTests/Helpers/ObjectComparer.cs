using System;

using GroBuf;
using GroBuf.DataMembersExtracters;

using NUnit.Framework;

namespace CassandraPrimitives.Tests.FunctionalTests.Helpers
{
    public static class ObjectComparer
    {
        public static void AssertEqualsToUsingGrobuf<T>(this T actual, T expected, string message = "", params object[] args)
        {
            var expectedBytes = serializer.Serialize(expected);
            var actualBytes = serializer.Serialize(actual);
            CollectionAssert.AreEqual(expectedBytes, actualBytes, message, args);
        }

        public static void AssertArrayEqualsTo<T>(this T[] actual, T[] expected, Action<T> beforeAssertAction = null)
        {
            Assert.AreEqual(expected.Length, actual.Length);
            for (var i = 0; i < expected.Length; i++)
            {
                if (beforeAssertAction != null)
                {
                    beforeAssertAction(expected[i]);
                    beforeAssertAction(actual[i]);
                }
                actual[i].AssertEqualsToUsingGrobuf(expected[i], "error at {0} index", i);
            }
        }

        private static readonly ISerializer serializer = new Serializer(new AllFieldsExtractor());
    }
}