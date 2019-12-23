using System;

namespace SkbKontur.Cassandra.Primitives.EventLog.Linq.Exceptions
{
    internal class CantMoveNextException : Exception
    {
        public CantMoveNextException(Exception innerException)
            : base("", innerException)
        {
        }
    }
}