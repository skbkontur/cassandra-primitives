using System;

namespace SkbKontur.Cassandra.Primitives.EventLog.Exceptions
{
    public class CouldNotWriteBoxEventException : Exception
    {
        public CouldNotWriteBoxEventException(string message)
            : base(message)
        {
        }
    }
}