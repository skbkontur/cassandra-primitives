using GroboSerializer.XmlNamespaces;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Settings
{
    public class EmptyXmlNamespaceFactory : IXmlNamespaceFactory
    {
        public XmlNamespace GetNamespace(string namespacePrefix)
        {
            return new XmlNamespace();
        }
    }
}