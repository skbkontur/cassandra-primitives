using System;
using System.Security.Cryptography;
using System.Text;

namespace SkbKontur.Cassandra.Primitives.EventLog.Sharding
{
    internal static class GuidFactory
    {
        public static Guid GetDeterministicGuid(this string input)
        {
            if (provider == null)
                provider = new MD5CryptoServiceProvider();
            var inputBytes = Encoding.UTF8.GetBytes(input ?? "");
            var hashBytes = provider.ComputeHash(inputBytes);
            var hashGuid = new Guid(hashBytes);
            return hashGuid;
        }

        [ThreadStatic]
        private static MD5CryptoServiceProvider provider;
    }
}