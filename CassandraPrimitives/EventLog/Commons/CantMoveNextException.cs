using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Commons
{
    public class CantMoveNextException : Exception
    {
        public CantMoveNextException(Exception innerException)
            : base("", innerException)
        {
        }
    }
}