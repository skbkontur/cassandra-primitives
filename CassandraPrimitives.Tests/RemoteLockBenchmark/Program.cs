using System;
using System.Diagnostics;
using System.IO;
using System.Net;
using System.Threading;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Catalogue.CassandraPrimitives.RemoteLock;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Settings;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.LocksFactory;
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
            var timeServiceProcess = Process.Start(new ProcessStartInfo
            {
                FileName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\TimeService\bin\Debug\Catalogue.CassandraPrimitives.Tests.TimeService.exe"),
                RedirectStandardOutput = false,
                UseShellExecute = true,
                WindowStyle = ProcessWindowStyle.Normal,
                CreateNoWindow = false,
            });
            var expirationServiceProcess = Process.Start(new ProcessStartInfo
            {
                FileName = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"..\..\..\ExpirationService\bin\Debug\Catalogue.CassandraPrimitives.Tests.ExpirationService.exe"),
                RedirectStandardOutput = false,
                UseShellExecute = true,
                WindowStyle = ProcessWindowStyle.Normal,
                CreateNoWindow = false,
            });
            try
            {
                cassandraClusterSettings = node.CreateSettings(IPAddress.Loopback);
                var initializerSettings = new CassandraInitializerSettings();
                var cassandraSchemeActualizer = new CassandraSchemeActualizer(new CassandraCluster(cassandraClusterSettings), new CassandraMetaProvider(), initializerSettings);
                cassandraSchemeActualizer.AddNewColumnFamilies();
                Log4NetConfiguration.InitializeOnce();
                var cassandraCluster = new CassandraCluster(cassandraClusterSettings);
                var oldRemoteLock = LocksCreatorFactory.CreateOldLock(cassandraCluster, ColumnFamilies.newRemoteLock);
                var newRemoteLockWithCassandraTTL = LocksCreatorFactory.CreateNewLockWithCassandraTTL(cassandraCluster, ColumnFamilies.newRemoteLock);
                var newRemoteLockWithExpirationService = LocksCreatorFactory.CreateNewLockWithExpirationService(cassandraCluster, ColumnFamilies.newRemoteLock);
                RunBenchmark(oldRemoteLock, "Old");
                RunBenchmark(newRemoteLockWithCassandraTTL, "NewCassandraTTL");
                RunBenchmark(newRemoteLockWithExpirationService, "NewExpirationService");
            }
            finally
            {
                if(timeServiceProcess != null) timeServiceProcess.Kill();
                if(expirationServiceProcess != null) expirationServiceProcess.Kill();
                node.Stop();
            }
        }

        private void RunBenchmark(IRemoteLockCreator remoteLockCreator, string lockName)
        {
            RunBenchmark(remoteLockCreator, lockName, 10, 20);
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
            for(var threadIndex = 0; threadIndex < threadsCount; threadIndex++)
                threads[threadIndex].Start(threadIndex);
            ms.Set();
            for(var threadIndex = 0; threadIndex < threadsCount; threadIndex++)
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