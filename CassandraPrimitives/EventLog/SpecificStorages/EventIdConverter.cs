using System;
using System.Text;

using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Exceptions;
using SKBKontur.Catalogue.CassandraPrimitives.EventLog.Primitives;
using SKBKontur.Catalogue.CassandraPrimitives.Storages.PersistentStorages;

namespace SKBKontur.Catalogue.CassandraPrimitives.EventLog.SpecificStorages
{
    public class EventIdConverter : ICassandraObjectIdConverter<EventInfo, EventId>
    {
        public EventId GetId(EventInfo obj)
        {
            return obj.Id;
        }

        public void CheckObjectIdentity(EventInfo obj)
        {
            if (obj == null) throw new ArgumentNullException("obj");
            if (obj.Id == null) throw new ArgumentNullException("obj.Id");
            if (string.IsNullOrEmpty(obj.Id.ScopeId)) throw new ArgumentNullException("obj.Id.ScopeId");
            if (string.IsNullOrEmpty(obj.Id.Id)) throw new ArgumentNullException("obj.Id.Id");
        }

        public string IdToRowKey(EventId identity)
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

        public EventId RowKeyToId(string rowKey)
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
            return new EventId
                {
                    ScopeId = builder.ToString(),
                    Id = id
                };
        }
    }
}