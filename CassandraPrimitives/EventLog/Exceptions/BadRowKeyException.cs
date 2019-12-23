using System;

namespace SkbKontur.Cassandra.Primitives.EventLog.Exceptions
{
    public class BadRowKeyException : Exception
    {
        public BadRowKeyException(string rowKey)
            : base("Bad rowKey " + rowKey)
        {
        }
    }
}