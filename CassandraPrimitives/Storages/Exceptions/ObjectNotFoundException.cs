using System;

namespace SkbKontur.Cassandra.Primitives.Storages.Exceptions
{
    public class ObjectNotFoundException : Exception
    {
        public ObjectNotFoundException(string format, params object[] parameters)
            : base(string.Format(format, parameters))
        {
        }
    }
}