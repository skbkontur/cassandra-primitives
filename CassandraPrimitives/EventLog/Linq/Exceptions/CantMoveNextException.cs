using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Linq.Exceptions
{
    internal class CantMoveNextException : Exception
    {
        public CantMoveNextException(Exception innerException)
            : base("", innerException)
        {
        }
    }
}