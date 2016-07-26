using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Security.Principal;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning
{
    [Obsolete]
    public class ImpersonateUser : IDisposable
    {
        public ImpersonateUser(string username, string domainOrComputerName, string password)
        {
            IntPtr userToken;

            var logonType = domainOrComputerName.ToLower().Equals("nt authority") ? LogonType.Service : LogonType.NewCredentials;

            if (!LogonUser(username, domainOrComputerName, password, (int)logonType, (int)LogonProvider.Default, out userToken))
                throw new Win32Exception();

            try
            {
                var identity = new WindowsIdentity(userToken);
                impersonationContext = identity.Impersonate();
            }
            finally
            {
                if (userToken != IntPtr.Zero)
                    CloseHandle(userToken);
            }
        }

        public ImpersonateUser(string username, string password)
            : this(username, Environment.UserDomainName, password)
        {
        }

        public ImpersonateUser(RemoteMachineCredentials credentials)
            : this(credentials.UserName, credentials.MachineName, credentials.Password) //TODO: Or account domain?
        {
        }

        [DllImport("advapi32.dll", SetLastError = true)]
        public static extern bool LogonUser(string lpszUsername, string lpszDomain, string lpszPassword, int dwLogonType, int dwLogonProvider, out IntPtr phToken);

        [DllImport("advapi32.dll", SetLastError = true)]
        public static extern bool ImpersonateLoggedOnUser(int token);

        [DllImport("advapi32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        public static extern bool DuplicateToken(IntPtr token, int impersonationLevel, ref IntPtr duplication);

        [DllImport("kernel32.dll")]
        public static extern bool CloseHandle(IntPtr hObject);

        internal enum LogonProvider
        {
            Default = 0,
            Winnt35 = 1,
        }

        internal enum LogonType
        {
            Interactive = 2,
            Network = 3,
            Batch = 4,
            Service = 5,
            Unlock = 7,
            NetworkCleartext = 8,
            NewCredentials = 9,
        }

        public void Dispose()
        {
            if (impersonationContext != null)
            {
                impersonationContext.Undo();
                impersonationContext.Dispose();
            }
        }

        private readonly WindowsImpersonationContext impersonationContext;
    }
}