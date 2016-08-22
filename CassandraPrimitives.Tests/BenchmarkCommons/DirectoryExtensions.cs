using System;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons
{
    public static class DirectoryExtensions
    {
        public static void CopyTo(this DirectoryInfo source, DirectoryInfo destination, bool overwrite = false)
        {
            if (!source.Exists)
                throw new ArgumentException(string.Format("Source directory ({0}) doesn't exist", source));
            if (destination.IsSubdorectoryOf(source))
                throw new ArgumentException(string.Format("Can't copy directory ({0}) to its subdirectory ({1})", source, destination));
            if (!destination.Exists)
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

        public static string GetMd5Hash(this DirectoryInfo directory, bool recursively)
        {
            using (var md5 = MD5.Create())
            {
                UdpateMd5Hash(directory, md5, recursively);
                md5.TransformFinalBlock(new byte[0], 0, 0);
                return string.Join("", md5.Hash.Select(x => x.ToString("x2")));
            }
        }

        private static string NormalisePath(this DirectoryInfo directory)
        {
            return directory.FullName.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar).ToLowerInvariant();
        }

        private static void UdpateMd5Hash(DirectoryInfo directory, MD5 md5, bool recursively)
        {
            var files = directory.GetFiles().OrderBy(f => f.Name).ToList();
            var filesJoined = Encoding.UTF8.GetBytes(string.Join("|", files.Select(f => f.Name)));
            md5.TransformBlock(filesJoined, 0, filesJoined.Length, filesJoined, 0);
            foreach (var file in files)
            {
                var data = File.ReadAllBytes(file.FullName);
                md5.TransformBlock(data, 0, data.Length, data, 0);
            }
            if (recursively)
            {
                var dirs = directory.GetDirectories().OrderBy(d => d.Name).ToList();
                foreach (var dir in dirs)
                {
                    var dirname = Encoding.UTF8.GetBytes(dir.Name);
                    md5.TransformBlock(dirname, 0, dirname.Length, dirname, 0);
                    UdpateMd5Hash(dir, md5, true);
                }
            }
        }
    }
}