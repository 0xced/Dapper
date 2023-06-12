using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Threading;

namespace Dapper.Tests
{
    public abstract class DatabaseProvider
    {
        public abstract DbProviderFactory Factory { get; }

        public abstract string GetConnectionString();

        protected string GetConnectionString(string name, string defaultConnectionString) =>
            Environment.GetEnvironmentVariable(name) ?? defaultConnectionString;

        public DbConnection GetOpenConnection()
        {
            var conn = Factory.CreateConnection();
            conn.ConnectionString = GetConnectionString();
            conn.Open();
            if (conn.State != ConnectionState.Open) throw new InvalidOperationException("should be open!");
            return conn;
        }

        public DbConnection GetClosedConnection()
        {
            var conn = Factory.CreateConnection();
            conn.ConnectionString = GetConnectionString();
            if (conn.State != ConnectionState.Closed) throw new InvalidOperationException("should be closed!");
            return conn;
        }

        public DbParameter CreateRawParameter(string name, object value)
        {
            var p = Factory.CreateParameter();
            p.ParameterName = name;
            p.Value = value ?? DBNull.Value;
            return p;
        }
    }

    public abstract class SqlServerDatabaseProvider : DatabaseProvider, IDisposable
    {
        public override string GetConnectionString() =>
            GetConnectionString("SqlServerConnectionString", "Data Source=.;Initial Catalog=tempdb;Integrated Security=True");

        public DbConnection GetOpenConnection(bool mars)
        {
            if (!mars) return GetOpenConnection();

            var scsb = Factory.CreateConnectionStringBuilder();
            scsb.ConnectionString = GetConnectionString();
            ((dynamic)scsb).MultipleActiveResultSets = true;
            var conn = Factory.CreateConnection();
            conn.ConnectionString = scsb.ConnectionString;
            conn.Open();
            if (conn.State != ConnectionState.Open) throw new InvalidOperationException("should be open!");
            return conn;
        }

        public void Dispose() { }
    }
    public sealed class SystemSqlClientProvider : SqlServerDatabaseProvider
    {
        public override DbProviderFactory Factory => System.Data.SqlClient.SqlClientFactory.Instance;
    }
#if MSSQLCLIENT
    public sealed class MicrosoftSqlClientProvider : SqlServerDatabaseProvider
    {
        public override DbProviderFactory Factory => Microsoft.Data.SqlClient.SqlClientFactory.Instance;
    }
#endif

    public abstract class TestBase<TProvider> : IDisposable where TProvider : DatabaseProvider, new()
    {
        public TestBase(TProvider provider)
        {
            Provider = provider;
        }

        protected void SkipIfMsDataClient()
            => Skip.If<Microsoft.Data.SqlClient.SqlConnection>(connection);

        protected DbConnection GetOpenConnection() => Provider.GetOpenConnection();
        protected DbConnection GetClosedConnection() => Provider.GetClosedConnection();
        protected DbConnection _connection;
        protected DbConnection connection => _connection ??= Provider.GetOpenConnection();

        public TProvider Provider { get; }

        protected static CultureInfo ActiveCulture
        {
            get { return Thread.CurrentThread.CurrentCulture; }
            set { Thread.CurrentThread.CurrentCulture = value; }
        }

        static TestBase()
        {
            Console.WriteLine("Dapper: " + typeof(SqlMapper).AssemblyQualifiedName);
            Console.WriteLine(".NET: " + Environment.Version);
            Console.Write("Loading native assemblies for SQL types...");
            try
            {
                SqlServerTypesLoader.LoadNativeAssemblies(AppDomain.CurrentDomain.BaseDirectory);
                Console.WriteLine("done.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("failed.");
                Console.Error.WriteLine(ex.Message);
            }
        }

        public virtual void Dispose()
        {
            _connection?.Dispose();
            _connection = null;
        }
    }

    public static class NonParallelDefinition
    {
        public const string Name = "NonParallel";
    }
}
