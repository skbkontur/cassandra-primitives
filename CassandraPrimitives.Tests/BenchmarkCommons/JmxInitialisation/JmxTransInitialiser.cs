using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.JmxInitialisation
{
    public class JmxTransInitialiser
    {
        public void DeployJmxTrans(string deployDirectory, List<JmxSettings> settingsList)
        {
            if (Directory.Exists(deployDirectory))
            {
                Console.WriteLine("Directory {0} already exists, deleting...", deployDirectory);
                Directory.Delete(deployDirectory, true);
                Console.WriteLine("Deleted existed directory {0}.", deployDirectory);
            }

            Directory.CreateDirectory(deployDirectory);
            Console.WriteLine("Deploying JmxTrans to {0}.", deployDirectory);
            var templateDirectory = FindJmxTransTemplateDirectory(AppDomain.CurrentDomain.BaseDirectory);
            Console.WriteLine("JmxTrans template directory: {0}.", templateDirectory);
            new DirectoryInfo(templateDirectory).CopyTo(new DirectoryInfo(deployDirectory), true);
            Console.WriteLine("JmxTrans deployed to {0}.", deployDirectory);

            Console.WriteLine("Patching settings...");
            PatchSettings(deployDirectory, settingsList);
            Console.WriteLine("Settings are patched");
        }

        public IDisposable RunJmxTrans(string jmxTransWorkDirectory, string wrapperPath)
        {
            using (var taskScheduler = new TaskSchedulerAdapter(wrapperPath))
            {
                var taskName = "BenchmarksJmxTrans";
                Console.WriteLine("Ensuring that JmxTrans is not started already");
                taskScheduler.StopAndDeleteTask(taskName);
                Console.WriteLine("Staring JmxTrans");
                taskScheduler.RunTaskInWrapper(taskName, Path.Combine(jmxTransWorkDirectory, "runJmxTrans.bat"), null, jmxTransWorkDirectory);
                Console.WriteLine("JmxTrans started");
                return new JmxTransStopper(taskName);
            }
        }

        private static string FindJmxTransTemplateDirectory(string currentDir)
        {
            if (currentDir == null)
                throw new Exception("Can't find directory with JmxTrans templates");
            var cassandraTemplateDirectory = Path.Combine(currentDir, @"Assemblies\JmxTrans");
            return Directory.Exists(cassandraTemplateDirectory) ? cassandraTemplateDirectory : FindJmxTransTemplateDirectory(Path.GetDirectoryName(currentDir));
        }

        private static void PatchSettings(string deployDirectory, List<JmxSettings> settingsList)
        {
            var serverSettings = GetPatchedSettingsList(Path.Combine(deployDirectory, @"conf\singleServerConfigTemplate.json"), settingsList);
            var joinedSettings = string.Join(",\n", serverSettings);
            var remoteConfigPath = Path.Combine(deployDirectory, @"config.json");
            var patchedConfig = File.ReadAllText(Path.Combine(deployDirectory, @"configTemplate.json")).Replace("{{servers}}", joinedSettings);
            File.WriteAllText(remoteConfigPath, patchedConfig);
        }

        private static Dictionary<string, string> GetMappingBySettings(JmxSettings settings)
        {
            return new Dictionary<string, string>
                {
                    {"numQueryThreads", settings.NumQueryThreads.ToString()},
                    {"alias", settings.Alias},
                    {"host", settings.Host},
                    {"port", settings.Port.ToString()},
                    {"graphiteHost", settings.GraphiteHost},
                    {"graphitePort", settings.GraphitePort.ToString()},
                    {"graphitePrefix", settings.GraphitePrefix},
                };
        }

        private static List<string> GetPatchedSettingsList(string templateServerConfig, List<JmxSettings> settingsList)
        {
            return settingsList
                .Select(GetMappingBySettings)
                .Select(mapping =>
                        mapping.Aggregate(
                            templateServerConfig,
                            (current, value) => current.Replace("{{" + value.Key + "}}", value.Value)))
                .ToList();
        }

        public class JmxTransStopper : IDisposable
        {
            public JmxTransStopper(string jmxTaskName)
            {
                this.jmxTaskName = jmxTaskName;
            }

            public void Dispose()
            {
                using (var taskScheduler = new TaskSchedulerAdapter(null))
                {
                    taskScheduler.StopAndDeleteTask(jmxTaskName);
                }
            }

            private readonly string jmxTaskName;
        }
    }
}