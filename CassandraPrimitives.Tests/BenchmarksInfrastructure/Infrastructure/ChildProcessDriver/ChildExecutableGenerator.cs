using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.IO;
using System.Linq;

using log4net;

using Metrics;

using Microsoft.CSharp;

using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarkCommons.Logging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ExternalLogging.HttpLogging;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.Registry;
using SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.TestConfigurations;

namespace SKBKontur.Catalogue.CassandraPrimitives.Tests.BenchmarksInfrastructure.Infrastructure.ChildProcessDriver
{
    public class ChildExecutableGenerator
    {
        public static void Generate(string outputDir, Func<IScenariosRegistry> registryCreator)
        {
            var logger = LogManager.GetLogger(typeof(ChildExecutableGenerator));
            var provider = new CSharpCodeProvider();
            Directory.CreateDirectory(outputDir);
            var compilerParameters = new CompilerParameters();
            foreach (var file in new DirectoryInfo(AppDomain.CurrentDomain.BaseDirectory).GetFiles())
                File.Copy(file.FullName, Path.Combine(outputDir, file.Name), true);
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                try
                {
                    compilerParameters.ReferencedAssemblies.Add(assembly.Location);
                    var f = new FileInfo(assembly.Location);
                    f.CopyTo(Path.Combine(outputDir, f.Name), true);
                }
                catch (NotSupportedException)
                {
                    //ignored: this happens for dynamic assemblies
                }
            }

            compilerParameters.GenerateExecutable = true;
            compilerParameters.OutputAssembly = Path.Combine(outputDir, "ChildRunner.exe");
            compilerParameters.GenerateInMemory = false;

            var compileUnit = new CodeCompileUnit();
            var codeNamespace = new CodeNamespace(typeof(ChildExecutableGenerator).Namespace + "." + "ChildExecutable");
            codeNamespace.Imports.Add(new CodeNamespaceImport("System"));
            codeNamespace.Imports.Add(new CodeNamespaceImport(typeof(ChildExecutableGenerator).Namespace));
            codeNamespace.Imports.Add(new CodeNamespaceImport(registryCreator.Method.DeclaringType.Namespace));
            codeNamespace.Imports.Add(new CodeNamespaceImport(typeof(IScenariosRegistry).Namespace));
            compileUnit.Namespaces.Add(codeNamespace);
            var program = new CodeTypeDeclaration("Program");
            codeNamespace.Types.Add(program);
            var start = new CodeEntryPointMethod();
            var initScenariosRegistry = new CodeVariableDeclarationStatement(
                "IScenariosRegistry",
                "scenariosRegistry",
                new CodeMethodInvokeExpression(
                    new CodeTypeReferenceExpression(registryCreator.Method.DeclaringType),
                    registryCreator.Method.Name));
            var run = new CodeMethodInvokeExpression(
                new CodeTypeReferenceExpression(typeof(ChildExecutableGenerator).FullName),
                "Run",
                new CodeVariableReferenceExpression("scenariosRegistry"));
            start.Statements.Add(initScenariosRegistry);
            start.Statements.Add(run);
            program.Members.Add(start);

            var cr = provider.CompileAssemblyFromDom(compilerParameters, compileUnit);

            if (cr.Errors.Count > 0)
            {
                logger.ErrorFormat("Errors building source into {0}", cr.PathToAssembly);
                Console.WriteLine("Errors building source into {0}", cr.PathToAssembly);
                foreach (CompilerError ce in cr.Errors)
                {
                    logger.ErrorFormat("  {0}", ce);
                    Console.WriteLine("  {0}", ce);
                }
            }
            else
            {
                logger.InfoFormat("Source built into {0} successfully.", cr.PathToAssembly);
                Console.WriteLine("Source built into {0} successfully.", cr.PathToAssembly);
            }
        }

        public static void Run(IScenariosRegistry scenariosRegistry)
        {
            Log4NetConfiguration.InitializeOnce();
            var logger = LogManager.GetLogger("ChildRunner");

            AppDomain.CurrentDomain.UnhandledException += (sender, e) => OnUnhandlingExceptionInChildProcess(sender, e, logger);

            InitMetrics();

            var args = Environment.GetCommandLineArgs().Skip(1).ToArray();
            if (args.Length < 3)
                throw new Exception("Not enough arguments");

            int processInd;
            if (!int.TryParse(args[0], out processInd))
                throw new Exception(string.Format("Invalid process id {0}", args[0]));

            logger.InfoFormat("Process id is {0}", processInd);
            logger.InfoFormat("Remote http address is {0}", args[1]);

            var processToken = args[2];

            TestConfiguration configuration;
            using (var httpExternalDataGetter = new HttpExternalDataGetter(args[1], 12345))
                configuration = httpExternalDataGetter.GetTestConfiguration().Result;
            logger.InfoFormat("Configuration was received");

            ChildProcessDriver.RunSingleTest(configuration, processInd, processToken, scenariosRegistry);
        }

        private static void InitMetrics()
        {
            Metric.SetGlobalContextName(string.Format("EDI.Benchmarks.ChildProcesses.{0}", Environment.MachineName));
            Metric.Config.WithHttpEndpoint("http://*:1234/").WithAllCounters();
            var graphiteUri = new Uri(string.Format("net.{0}://{1}:{2}", "tcp", "graphite-relay.skbkontur.ru", "2003"));
            Metric.Config.WithReporting(x => x
                                                 .WithGraphite(graphiteUri, TimeSpan.FromSeconds(5))
                                                 .WithCSVReports(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs", "csv"), TimeSpan.FromMinutes(1), ";")
                                                 .WithTextFileReport(Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "MetricsLogs", "textMetrics.txt"), TimeSpan.FromMinutes(1)));
        }

        private static void OnUnhandlingExceptionInChildProcess(object sender, UnhandledExceptionEventArgs e, ILog logger)
        {
            logger.FatalFormat("Unhandled exception in child process:\n{0}", e.ExceptionObject);
        }
    }
}