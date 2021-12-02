# Changelog

## v3.1.5 - 2021.12.02
- Update dependencies.
- Run tests against net6.0 tfm.

## v3.1.4 - 2021.03.14
- Update dependencies.
- Run tests against net5.0 tfm.

## v3.1.2 - 2020.08.26
- Breaking changes: `IEventRepositoryFactory` requires `eventsTtl` parameter, all event log records are written with the ttl.
- Breaking changes: `IEventRepositoryFactory` requires `IEventLogProfiler` parameter, `EventLogNullProfiler` is now a public class.

## v2.2.10 - 2019.12.24
- Adjust root namespace name to match assembly name.
- Move `MinTicksHolder` and `MaxTicksHolder` to [Cassandra.GlobalTimestamp](https://github.com/skbkontur/cassandra-global-timestamp) repository.
- Remove `TicksHolder` and `GlobalTime` since `TicksHolder` column family was never updated.

## v2.2.4 - 2019.11.17
- Use precise monotonic timestamp from [SkbKontur.Cassandra.TimeGuid](https://github.com/skbkontur/cassandra-time-guid) package.
- Use [SourceLink](https://github.com/dotnet/sourcelink) to help ReSharper decompiler show actual code.

## v2.1.3 - 2019.10.12
- Target .NET Standard 2.0 (PR [#2](https://github.com/skbkontur/cassandra-primitives/pull/2)).

## v2.0 - 2018.09.12
- Use [SkbKontur.Cassandra.ThriftClient](https://github.com/skbkontur/cassandra-thrift-client) and
[SkbKontur.Cassandra.DistributedLock](https://github.com/skbkontur/cassandra-distributed-lock) packages from NuGet.
- Use [SkbKontur.Cassandra.Local](https://github.com/skbkontur/cassandra-local) module for integration testing.
- Switch to SDK-style project format and dotnet core build tooling.
- Set TargetFramework to net471.
- Use [Vostok.Logging.Abstractions](https://github.com/vostok/logging.abstractions) as a logging framework facade.
- Use [Nerdbank.GitVersioning](https://github.com/AArnott/Nerdbank.GitVersioning) to automate generation of assembly 
  and nuget package versions.
- Implement workaround for "ThreadAbortException not re-thrown by the runtime" 
  [issue](https://github.com/dotnet/coreclr/issues/16122) on net471.
