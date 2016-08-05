using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.JmxInitialisation
{
    public class JmxTransInitialiser
    {
        public JmxTransInitialiser(string taskGroup)
        {
            this.taskGroup = taskGroup;
        }

        public void DeployJmxTrans(string deployDirectory, List<JmxSettings> settingsList)
        {
            using (var taskScheduler = new TaskSchedulerAdapter(null, taskGroup))
            {
                taskScheduler.StopAndDeleteTask(taskName);
            }
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
            using (var taskScheduler = new TaskSchedulerAdapter(wrapperPath, taskGroup))
            {
                Console.WriteLine("Staring JmxTrans");
                taskScheduler.RunTaskInWrapper(taskName, Path.Combine(jmxTransWorkDirectory, "runJmxTrans.bat"), null, jmxTransWorkDirectory);
                Console.WriteLine("JmxTrans started");
                return new JmxTransStopper(taskName, taskGroup);
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
            var serverConfigTemplate = File.ReadAllText(Path.Combine(deployDirectory, @"singleServerConfigTemplate.json"));
            var serverSettings = GetPatchedSettingsList(serverConfigTemplate, settingsList);
            var joinedSettings = string.Join(",\n", serverSettings);
            var remoteConfigPath = Path.Combine(deployDirectory, @"config.json");
            var patchedConfig = File.ReadAllText(Path.Combine(deployDirectory, @"configTemplate.json")).Replace("{{servers}}", joinedSettings);
            File.WriteAllText(remoteConfigPath, patchedConfig);
        }

        private static Dictionary<string, string> GetMappingBySettings(JmxSettings settings)
        {
            return new Dictionary<string, string>
                {
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

        private const string taskName = "JmxTrans";
        private readonly string taskGroup;

        public class JmxTransStopper : IDisposable
        {
            public JmxTransStopper(string jmxTaskName, string jmxTaskGroup)
            {
                this.jmxTaskName = jmxTaskName;
                this.jmxTaskGroup = jmxTaskGroup;
            }

            public void Dispose()
            {
                using (var taskScheduler = new TaskSchedulerAdapter(null, jmxTaskGroup))
                {
                    taskScheduler.StopAndDeleteTask(jmxTaskName);
                }
            }

            private readonly string jmxTaskName, jmxTaskGroup;
        }
    }
}