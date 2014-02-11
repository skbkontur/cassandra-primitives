using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.Storages.Exceptions
{
    public class ObjectNotFoundException : Exception
    {
        public ObjectNotFoundException(string format, params object[] parameters)
            : base(string.Format(format, parameters))
        {
        }
    }
}