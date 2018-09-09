using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Exceptions
{
    public class BadRowKeyException : Exception
    {
        public BadRowKeyException(string rowKey)
            : base("Bad rowKey " + rowKey)
        {
        }
    }
}