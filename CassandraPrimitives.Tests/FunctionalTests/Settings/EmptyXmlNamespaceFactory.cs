using GroboSerializer.XmlNamespaces;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings
{
    public class EmptyXmlNamespaceFactory : IXmlNamespaceFactory
    {
        public XmlNamespace GetNamespace(string namespacePrefix)
        {
            return new XmlNamespace();
        }
    }
}