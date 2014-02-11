using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace SKBKontur.Catalogue.CassandraPrimitives.FunctionalTests.Helpers
{
    public static class AssembliesLoader
    {
        public static IEnumerable<Assembly> Load()
        {
            string searchPath = AppDomain.CurrentDomain.RelativeSearchPath;
            if (string.IsNullOrEmpty(searchPath))
                searchPath = AppDomain.CurrentDomain.BaseDirectory;
            return Directory.EnumerateFiles(searchPath, "*", SearchOption.TopDirectoryOnly)
                            .Where(IsOurAssembly)
                            .Select(Assembly.LoadFrom)
                            .ToArray();
        }

        private static bool IsOurAssembly(string fullFileName)
        {
            var fileName = Path.GetFileName(fullFileName);
            if (string.IsNullOrEmpty(fileName))
                return false;
            return (fileName.EndsWith(".dll", StringComparison.InvariantCultureIgnoreCase) ||
                    fileName.EndsWith(".exe", StringComparison.InvariantCultureIgnoreCase))
                   &&
                   (fileName.StartsWith("SKBKontur.", StringComparison.InvariantCultureIgnoreCase) ||
                    fileName.StartsWith("Catalogue.", StringComparison.InvariantCultureIgnoreCase) ||
                    fileName.StartsWith("GroboSerializer", StringComparison.InvariantCultureIgnoreCase) ||
                    fileName.StartsWith("GroBuf", StringComparison.InvariantCultureIgnoreCase) ||
                    fileName.StartsWith("Cassandra.", StringComparison.InvariantCultureIgnoreCase) ||
                    fileName.StartsWith("RemoteTaskQueue.", StringComparison.InvariantCultureIgnoreCase) ||
                    fileName.StartsWith("RemoteLock.", StringComparison.InvariantCultureIgnoreCase));
        }
    }
}