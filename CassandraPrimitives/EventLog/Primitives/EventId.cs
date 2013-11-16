using System;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives
{
    public class EventId : IComparable<EventId>
    {
        public int CompareTo(EventId other)
        {
            var scopeIdComparisonResult = String.Compare(ScopeId, other.ScopeId, StringComparison.Ordinal);
            if(scopeIdComparisonResult != 0) return scopeIdComparisonResult;
            return String.Compare(Id, other.Id, StringComparison.Ordinal);
        }

        public override bool Equals(object obj)
        {
            if(ReferenceEquals(null, obj)) return false;
            if(ReferenceEquals(this, obj)) return true;
            if(obj.GetType() != GetType()) return false;
            return Equals((EventId)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((ScopeId != null ? ScopeId.GetHashCode() : 0) * 397) ^ (Id != null ? Id.GetHashCode() : 0);
            }
        }

        public string ScopeId { get; set; }
        public string Id { get; set; }

        protected bool Equals(EventId other)
        {
            return string.Equals(ScopeId, other.ScopeId) && string.Equals(Id, other.Id);
        }
    }
}