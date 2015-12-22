﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.FunctionalTests.Helpers
{
    public static class AssembliesLoader
    {
        public static IEnumerable<Assembly> Load()
        {
            return EnumerateFiles(AppDomain.CurrentDomain.BaseDirectory)
                .Concat(EnumerateFiles(AppDomain.CurrentDomain.RelativeSearchPath))
                .Distinct()
                .Where(IsOurAssembly)
                .Select(Assembly.LoadFrom)
                .ToArray();
        }

        private static IEnumerable<string> EnumerateFiles(string dir)
        {
            return string.IsNullOrWhiteSpace(dir)
                       ? new string[0]
                       : Directory.EnumerateFiles(dir, "*", SearchOption.TopDirectoryOnly);
        }

        private static bool IsOurAssembly(string fullFileName)
        {
            var fileName = Path.GetFileName(fullFileName);
            if(string.IsNullOrEmpty(fileName))
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