using System;
using System.IO;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark.ExternalLogging
{
    public class FileLoggingTools
    {
        public static string CreateLogFileAndGetPath(string workingDirectory, int processInd)
        {
            var logDir = Path.Combine(workingDirectory, "ExternalLogging");
            if (!Directory.Exists(logDir))
                Directory.CreateDirectory(logDir);
            var filename = String.Format("log_proc_{0}.txt", processInd);
            return Path.Combine(logDir, filename);
        }

        public static string GetLogFilePath(string workingDirectory, int processInd)
        {
            var logDir = Path.Combine(workingDirectory, "ExternalLogging");
            if (!Directory.Exists(logDir))
                throw new Exception(String.Format("External logs directory ({0}) doesn't exist", logDir));
            var filename = String.Format("log_proc_{0}.txt", processInd);
            return Path.Combine(logDir, filename);
        }
    }
}