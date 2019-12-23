namespace SkbKontur.Cassandra.Primitives.Storages.Primitives
{
    public class ColumnFamilyFullName
    {
        public ColumnFamilyFullName(string keyspaceName, string columnFamilyName)
        {
            KeyspaceName = keyspaceName;
            ColumnFamilyName = columnFamilyName;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((ColumnFamilyFullName)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((KeyspaceName != null ? KeyspaceName.GetHashCode() : 0) * 397) ^ (ColumnFamilyName != null ? ColumnFamilyName.GetHashCode() : 0);
            }
        }

        public string KeyspaceName { get; private set; }
        public string ColumnFamilyName { get; private set; }

        private bool Equals(ColumnFamilyFullName other)
        {
            return string.Equals(KeyspaceName, other.KeyspaceName) && string.Equals(ColumnFamilyName, other.ColumnFamilyName);
        }
    }
}