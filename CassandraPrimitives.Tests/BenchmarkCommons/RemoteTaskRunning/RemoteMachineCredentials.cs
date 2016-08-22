namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.RemoteTaskRunning
{
    public class RemoteMachineCredentials
    {
        public RemoteMachineCredentials(string machineName, string userName = null, string accountDomain = null, string password = null)
        {
            MachineName = machineName;
            UserName = userName;
            AccountDomain = accountDomain;
            Password = password;
        }

        public string MachineName { get; private set; }
        public string UserName { get; private set; }
        public string AccountDomain { get; private set; }
        public string Password { get; private set; }
    }
}