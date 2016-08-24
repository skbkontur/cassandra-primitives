using System;
using System.Collections.Generic;
using System.Net;

using Cassandra;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.CasRemoteLock
{
    public class CasRemoteLockProvider
    {
        private readonly string tableName;
        private readonly ISession session;
        private readonly ConsistencyLevel consistencyLevel;

        public CasRemoteLockProvider(List<IPEndPoint> endpoints, string keyspaceName, string tableName, ConsistencyLevel consistencyLevel)
        {
            this.tableName = tableName;
            this.consistencyLevel = consistencyLevel;
            var cluster = Cluster
                .Builder()
                .AddContactPoints(endpoints)
                .Build();
            session = cluster.Connect(keyspaceName);
        }

        public void ActualiseTables()
        {
            //session.Execute(string.Format("DROP TABLE \"{0}\";", tableName), consistencyLevel);//TODO
            //if (session.Execute("SELECT * FROM system.schema_columnfamilies;", consistencyLevel).Any(row => row.GetValue<string>("columnfamily_name") == tableName))
            //    return;
            try
            {
                session.Execute(string.Format("CREATE TABLE \"{0}\" (", tableName) +
                                            "lock_id text PRIMARY KEY," +
                                            "owner text," +
                                            ");", consistencyLevel);
            }
            catch (Exception)
            {
                //ignore
            }
        }

        public CasRemoteLocker CreateLocker(TimeSpan lockTtl)
        {
            return new CasRemoteLocker(session, tableName, lockTtl, consistencyLevel);
        }
    }
}