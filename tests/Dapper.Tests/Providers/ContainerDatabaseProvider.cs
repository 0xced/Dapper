using System;
using System.Threading.Tasks;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Xunit;

namespace Dapper.Tests
{
    public abstract class ContainerDatabaseProvider<TBuilder, TContainer> : DatabaseProvider, IAsyncLifetime
        where TBuilder : IContainerBuilder<TBuilder, TContainer>, new()
        where TContainer : IContainer, IDatabaseContainer
    {
        private string _connectionString;
        private TContainer _container;

        protected abstract string ProviderName { get; }

        public override string GetConnectionString()
        {
            return _connectionString ?? throw new InvalidOperationException($"{nameof(IAsyncLifetime.InitializeAsync)} must be called before accessing the connection string");
        }

        async Task IAsyncLifetime.InitializeAsync()
        {
            Console.WriteLine($"Using Provider: {Factory.GetType().FullName}");

            var environmentVariableName = $"DapperTests_{ProviderName}_ConnectionString";
            var connectionString = Environment.GetEnvironmentVariable(environmentVariableName);
            if (connectionString != null)
            {
                _connectionString = connectionString;
                Console.WriteLine($"Using ConnectionString: {_connectionString}");
            }

            try
            {
                if (_connectionString == null)
                {
                    _container = new TBuilder().Build();
                    await _container.StartAsync();
                    _connectionString = _container.GetConnectionString();
                    Console.WriteLine($"Using ConnectionString: {_connectionString}");
                }
                using (GetOpenConnection()) { /* just trying to see if it works */ }
            }
            catch (Exception ex)
            {
                Skip.Inconclusive($"{ProviderName} is unavailable: {ex.Message}");
            }

        }

        async Task IAsyncLifetime.DisposeAsync()
        {
            if (_container != null)
            {
                await _container.StopAsync();
            }
        }
    }
}
