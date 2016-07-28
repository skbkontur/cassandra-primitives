using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.ZookeeperInitialisaton
{
    public static class ZookeeperDeployer
    {
        public static void Deploy(ZookeeperNodeSettings settings, string deployDirectory)
        {
            if (Directory.Exists(deployDirectory))
            {
                Console.WriteLine("Directory {0} already exists, deleting...", deployDirectory);
                Directory.Delete(deployDirectory, true);
                Console.WriteLine("Deleted existed directory {0}.", deployDirectory);
            }

            Directory.CreateDirectory(deployDirectory);
            Console.WriteLine("Deploying zookeeper to {0}.", deployDirectory);
            var templateDirectory = FindZookeeperTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory);
            Console.WriteLine("Zookeeper template directory: {0}.", templateDirectory);
            new DirectoryInfo(templateDirectory).CopyTo(new DirectoryInfo(deployDirectory), true);
            Console.WriteLine("Zookeeper deployed to {0}.", deployDirectory);

            Console.WriteLine("Patching settings...");
            PatchSettings(deployDirectory, settings);
            Console.WriteLine("Settings are patched");
        }

        private static string FindZookeeperTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new Exception("Can't find directory with Zookeeper templates");
            var cassandraTemplateDirectory = Path.Combine(currentDir, @"Assemblies\ZookeeperTemplates");
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindZookeeperTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private static void PatchSettings(string deployDirectory, ZookeeperNodeSettings settings)
        {
            PatchSettingsInFile(Path.Combine(deployDirectory, @"conf\zoo.cfg"), settings);
            PatchMyid(Path.Combine(deployDirectory, @"data\myid"), settings.Id);
            PatchServerAddressesInFile(Path.Combine(deployDirectory, @"conf\zoo.cfg"), settings);
        }

        private static string SafeToString(object obj)
        {
            return obj == null ? null : obj.ToString();
        }

        private static void PatchSettingsInFile(string filePath, ZookeeperNodeSettings settings)
        {
            PatchSettingsInFile(filePath, new Dictionary<string, string>
                {
                    {"tickTime", SafeToString(settings.TickTime)},
                    {"initLimit", SafeToString(settings.InitLimit)},
                    {"syncLimit", SafeToString(settings.SyncLimit)},
                    {"dataDir", settings.DataDir},
                    {"clientPort", SafeToString(settings.ClientPort)},
                    {"maxClientCnxns", SafeToString(settings.MaxClientCnxns)},
                    {"autopurge.snapRetainCount", SafeToString(settings.AutopurgeSnapRetainCount)},
                    {"autopurge.purgeInterval", SafeToString(settings.AutopurgePurgeInterval)},
                });
        }

        private static void PatchSettingsInFile(string filePath, Dictionary<string, string> values)
        {
            File.WriteAllText(
                filePath,
                values.Aggregate(
                    File.ReadAllText(filePath),
                    (current, value) => current.Replace("{{" + value.Key + "}}", value.Value != null ? string.Format("{0}={1}", value.Key, value.Value) : string.Format("#{0}", value.Key)))
                );
        }

        private static void PatchServerAddressesInFile(string filePath, ZookeeperNodeSettings settings)
        {
            var servers = "#servers";
            if (settings.ServerAddresses != null)
                servers = string.Join("\n", settings.ServerAddresses.Select((addr, i) => string.Format("server.{0}={1}:2888:3888", i + 1, addr)));
            File.WriteAllText(filePath, File.ReadAllText(filePath).Replace("{{servers}}", servers));
        }

        private static void PatchMyid(string filePath, int? id)
        {
            if (id != null)
                File.WriteAllText(filePath, id.ToString());
        }
    }
}