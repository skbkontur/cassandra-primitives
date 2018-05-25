using System;
using System.IO;
using System.Text;

using Vostok.Logging;
using Vostok.Logging.Configuration;
using Vostok.Logging.Configuration.Settings;
using Vostok.Logging.Logs;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests
{
    public static class Logger
    {
        public static ILog Instance => log ?? (log = InitFileLogger());

        private static ILog InitFileLogger()
        {
            var logsDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs");
            if(!Directory.Exists(logsDir))
                Directory.CreateDirectory(logsDir);
            FileLog.Configure(() => new FileLogSettings
                {
                    AppendToFile = false,
                    EnableRolling = false,
                    Encoding = Encoding.UTF8,
                    ConversionPattern = ConversionPattern.Default,
                    FilePath = Path.Combine(logsDir, $"FunctionalTests-{DateTime.Now:yyyy-MM-dd.HH-mm-ss}.log"),
                });
            return new FileLog();
        }

        private static ILog log;
    }
}