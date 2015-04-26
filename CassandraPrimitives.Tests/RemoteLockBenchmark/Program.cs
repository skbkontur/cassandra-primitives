using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;

using GroBuf;
using GroBuf.DataMembersExtracters;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.LockCreatorStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.LockStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.MetaStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.QueueStorage;
using SKBKontur.Catalogue.CassandraPrimitives.NewRemoteLock.RentExtender;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.SchemeActualizer;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.ThreadsBasedRemoteLockBenchmark
{
    internal class Program
    {
        public static void Main(string[] args)
        {
            new Program().Run();
        }

        private void Run()
        {
            var node = CreateCassandraNode();
            node.Restart();
            try
            {
                cassandraClusterSettings = node.CreateSettings(IPAddress.Loopback);
                var initializerSettings = new CassandraInitializerSettings();
                var cassandraSchemeActualizer = new CassandraSchemeActualizer(new CassandraCluster(cassandraClusterSettings), new CassandraMetaProvider(), initializerSettings);
                cassandraSchemeActualizer.AddNewColumnFamilies();
                Log4NetConfiguration.InitializeOnce();
                var oldRemoteLock = CreateOldRemoteLock();
                var newRemoteLockCassandra = CreateNewRemoteLockCassandra();
                RunBenchmark(oldRemoteLock, "Old");
                RunBenchmark(newRemoteLockCassandra, "New cassandra");
            }
            finally
            {
                node.Stop();
            }
        }

        private NewRemoteLockCreator CreateNewRemoteLockCassandra()
        {
            var cassandraCluster = new CassandraCluster(cassandraClusterSettings);
            var serializer = new Serializer(new AllPropertiesExtractor());
            var columnFamilyFullName = ColumnFamilies.newRemoteLock;
            var remoteLockSettings = new RemoteLockSettings(columnFamilyFullName.KeyspaceName, columnFamilyFullName.ColumnFamilyName);
            var metaStorage = new MetaStorage(cassandraCluster, serializer, remoteLockSettings);
            var lockStorage = new LockStorage(metaStorage, cassandraCluster, remoteLockSettings);
            var queueStorage = new QueueStorage(metaStorage, cassandraCluster, serializer, remoteLockSettings);
            var rentExtender = new RentExtender(queueStorage, lockStorage);
            return new NewRemoteLockCreator(new LockCreatorStorage(lockStorage, queueStorage, rentExtender), remoteLockSettings);
        }

        private RemoteLockCreator CreateOldRemoteLock()
        {
            var cassandraCluster = new CassandraCluster(cassandraClusterSettings);
            var serializer = new Serializer(new AllPropertiesExtractor());
            var columnFamilyFullName = ColumnFamilies.remoteLock;
            var remoteLockImplementation = new CassandraRemoteLockImplementation(cassandraCluster, serializer, columnFamilyFullName);
            return new RemoteLockCreator(remoteLockImplementation);
        }

        private void RunBenchmark(IRemoteLockCreator remoteLockCreator, string lockName)
        {
            //RunBenchmark(remoteLockCreator, lockName, 1, 10000);
            //RunBenchmark(remoteLockCreator, lockName, 2, 5000);
            RunBenchmark(remoteLockCreator, lockName, 60, 20);
        }

        private void RunBenchmark(IRemoteLockCreator remoteLockCreator, string lockName, int threadsCount, int locksCount)
        {
            var lockId = Guid.NewGuid().ToString();
            var sw = new Stopwatch();
            var threads = new Thread[threadsCount];
            var ms = new ManualResetEvent(false);
            for(var threadIndex = 0; threadIndex < threadsCount; threadIndex++)
            {
                threads[threadIndex] = new Thread((index) =>
                {
                    ms.WaitOne();
                    for(var i = 0; i < locksCount; i++)
                    {
                        using(remoteLockCreator.Lock(lockId))
                        {
                            Thread.Sleep(20);
                            Console.WriteLine("Take lock by thread {0}, number {1}", index, i);
                        }
                        Thread.Sleep(10);
                    }
                });
            }
            sw.Start();
            for (int threadIndex = 0; threadIndex < threadsCount; threadIndex++) 
                threads[threadIndex].Start(threadIndex);
            ms.Set();
            for(int threadIndex = 0; threadIndex < threadsCount; threadIndex++)
                threads[threadIndex].Join();
            sw.Stop();
            Console.WriteLine("{0}, threads {1}, locks {2}: {3}", lockName, threadsCount, locksCount, sw.ElapsedMilliseconds);
        }

        private CassandraNode CreateCassandraNode()
        {
            return new CassandraNode(Path.Combine(FindCassandraTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory), @"1.2"))
            {
                Name = "node_at_9360",
                JmxPort = 7399,
                GossipPort = 7400,
                RpcPort = 9360,
                CqlPort = 9343,
                DataBaseDirectory = @"../data/",
                DeployDirectory = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\Cassandra1.2"),
                ListenAddress = "127.0.0.1",
                RpsAddress = "0.0.0.0",
                SeedAddresses = new[] {"127.0.0.1"},
                InitialToken = "",
                ClusterName = "test_cluster"
            };
        }

        private string FindCassandraTemplateDirectory(string currentDir)
        {
            if(currentDir == null)
                throw new Exception("Невозможно найти каталог с Cassandra-шаблонами");
            var cassandraTemplateDirectory = Path.Combine(currentDir, cassandraTemplates);
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindCassandraTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private ICassandraClusterSettings cassandraClusterSettings;
        private const string cassandraTemplates = @"Assemblies\CassandraTemplates";
    }
}