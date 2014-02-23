using System;

using GroBuf;
using GroBuf.DataMembersExtracters;

using GroboSerializer;

using NUnit.Framework;

using SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Settings;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Helpers
{
    public static class ObjectComparer
    {
        public static void AssertEqualsToWithXmlSerializer<T>(this T actual, T expected, string message = "", params object[] args)
        {
            var xmlSerializer = new XmlSerializer(new EmptyXmlNamespaceFactory());
            var badXml = XmlFormatter.ReformatXml("<root></root>");
            var expectedStr = xmlSerializer.SerializeToUtfString(expected, true);
            Assert.AreNotEqual(XmlFormatter.ReformatXml(expectedStr), badXml, "bug(expected)");
            var actualStr = xmlSerializer.SerializeToUtfString(actual, true);
            Assert.AreNotEqual(XmlFormatter.ReformatXml(actualStr), badXml, "bug(actual)");
            if(expectedStr != actualStr)
            {
                Console.WriteLine("Expected: \n\r" + xmlSerializer.SerializeToUtfString(expected, true));
                Console.WriteLine("Actual: \n\r" + xmlSerializer.SerializeToUtfString(actual, true));
            }
            if(string.IsNullOrEmpty(message))
                Assert.AreEqual(expectedStr, actualStr);
            else
                Assert.AreEqual(expectedStr, actualStr, message, args);
        }

        public static void AssertEqualsToUsingGrobuf<T>(this T actual, T expected, string message = "", params object[] args)
        {
            var expectedBytes = serializer.Serialize(expected);
            var actualBytes = serializer.Serialize(actual);
            CollectionAssert.AreEqual(expectedBytes, actualBytes, message, args);
        }

        public static void AssertEqualsXml(this string actual, string expected, string message = "", params object[] args)
        {
            if(string.IsNullOrEmpty(message))
                Assert.AreEqual(XmlFormatter.ReformatXml(expected), XmlFormatter.ReformatXml(actual));
            else
                Assert.AreEqual(XmlFormatter.ReformatXml(expected), XmlFormatter.ReformatXml(actual), message, args);
        }

        public static void AssertArrayEqualsTo<T>(this T[] actual, T[] expected, Action<T> beforeAssertAction = null)
        {
            Assert.AreEqual(expected.Length, actual.Length);
            for(var i = 0; i < expected.Length; i++)
            {
                if(beforeAssertAction != null)
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