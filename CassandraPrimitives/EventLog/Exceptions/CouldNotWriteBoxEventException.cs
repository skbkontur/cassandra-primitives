using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Exceptions
{
    public class CouldNotWriteBoxEventException : Exception
    {
        public CouldNotWriteBoxEventException(string message)
            : base(message)
        {
        }
    }
}