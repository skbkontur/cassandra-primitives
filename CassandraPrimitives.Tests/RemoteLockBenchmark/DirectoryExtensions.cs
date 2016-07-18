using System;
using System.IO;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.RemoteLockBenchmark
{
    public static class DirectoryExtensions
    {
        public static void CopyTo(this DirectoryInfo source, DirectoryInfo destination, bool overwrite = false)
        {
            if (destination.IsSubdorectoryOf(source))
                throw new ArgumentException(String.Format("Can't copy directory ({0}) to its subdirectory ({1})", source, destination));
            destination.Create();
            foreach (var file in source.EnumerateFiles())
                file.CopyTo(Path.Combine(destination.FullName, file.Name), overwrite);
            foreach (var directory in source.EnumerateDirectories())
                directory.CopyTo(new DirectoryInfo(Path.Combine(destination.FullName, directory.Name)), overwrite);
        }

        public static bool IsSubdorectoryOf(this DirectoryInfo first, DirectoryInfo second)
        {
            while (first != null)
            {
                if (first.ProperEquals(second))
                    return true;
                first = first.Parent;
            }
            return false;
        }

        public static bool ProperEquals(this DirectoryInfo first, DirectoryInfo second)
        {
            if (first == null || second == null)
                return false;
            return first.NormalisePath().Equals(second.NormalisePath());
        }

        private static string NormalisePath(this DirectoryInfo directory)
        {
            return directory.FullName.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).ToLowerInvariant();
        }
    }
}