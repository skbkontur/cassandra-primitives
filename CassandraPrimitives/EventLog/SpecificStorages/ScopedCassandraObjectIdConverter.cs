using System;
using System.Text;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Exceptions;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.PersistentStorages;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.SpecificStorages
{
    internal class ScopedCassandraObjectIdConverter<T> : ICassandraObjectIdConverter<T, ScopedCassandraObjectId>
        where T : class, IScopedCassandraObject
    {
        public ScopedCassandraObjectId GetId(T obj)
        {
            return new ScopedCassandraObjectId
                {
                    ScopeId = obj.ScopeId,
                    Id = obj.Id,
                };
        }

        public void CheckObjectIdentity(T obj)
        {
            if (obj == null) throw new ArgumentNullException("obj");
            if (string.IsNullOrEmpty(obj.ScopeId)) throw new ArgumentNullException("obj.ScopeId");
            if (string.IsNullOrEmpty(obj.Id)) throw new ArgumentNullException("obj.Id");
        }

        public string IdToRowKey(ScopedCassandraObjectId identity)
        {
            var scopeId = identity.ScopeId;
            var id = identity.Id;

            var builder = new StringBuilder();
            for (var i = 0; i < scopeId.Length; ++i)
            {
                if (scopeId[i] == '_' || scopeId[i] == '\\')
                {
                    builder.Append('\\');
                    builder.Append(scopeId[i]);
                }
                else
                {
                    builder.Append(scopeId[i]);
                }

            }
            return string.Format("{0}_{1}", builder, id);
        }

        public ScopedCassandraObjectId RowKeyToId(string rowKey)
        {
            var builder = new StringBuilder();
            string id = null;
            for (var i = 0; i < rowKey.Length; ++i)
            {
                if (rowKey[i] == '_')
                {
                    id = rowKey.Substring(i + 1);
                    break;
                }
                if (rowKey[i] == '\\')
                {
                    ++i;
                    if (i >= rowKey.Length)
                        throw new BadRowKeyException(rowKey);
                    builder.Append(rowKey[i]);
                }
                else
                    builder.Append(rowKey[i]);
            }
            if (id == null)
                throw new BadRowKeyException(rowKey);
            return new ScopedCassandraObjectId
                {
                    ScopeId = builder.ToString(),
                    Id = id
                };
        }
    }
}