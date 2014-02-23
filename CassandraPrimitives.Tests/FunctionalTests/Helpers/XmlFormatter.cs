using System.Linq;
using System.Text;
using System.Xml;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers
{
    public static class XmlFormatter
    {
        public static string ReformatXml(string xml)
        {
            var document = new XmlDocument();
            document.LoadXml(xml);
            var result = new StringBuilder();
            XmlWriter writer = XmlWriter.Create(result, new XmlWriterSettings
                {
                    Indent = true,
                    OmitXmlDeclaration = !HasXmlDeclaration(document)
                });
            document.WriteTo(writer);
            writer.Flush();
            return result.ToString();
        }

        private static bool HasXmlDeclaration(XmlDocument document)
        {
            return document.ChildNodes.OfType<XmlDeclaration>().Any();
        }
    }
}