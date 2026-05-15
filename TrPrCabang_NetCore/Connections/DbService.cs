using Dapper;
using Microsoft.Extensions.Configuration;
using MySqlConnector;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Connection
{
    public interface IDbServices
    {
        string DatabaseName();
        IDbConnection CreateConnection();
        Task<List<T>> FindListAsync<T>(string cmd, object? parameter = null);
        List<T> FindList<T>(string cmd, object? parameter = null);
        Task<T?> ExecuteScalarAsync<T>(string cmd, object? parameter = null);
        T? ExecuteScalar<T>(string cmd, object? parameter = null);
    }

    public class DbService : IDbServices
    {
        public static string? _connectionString;
        public static long ID_Receiver_Email_Notif = 3;
        private MySqlConnection? SharedConnection { get; set; }
        private MySqlTransaction? DBTrans { get; set; }
        private static readonly ConcurrentDictionary<int, bool> _initializedSessions = new();
        private readonly int _CommandTimeout = 1000;
        private readonly SemaphoreSlim _transactionLock = new SemaphoreSlim(1, 1);
        public DbService(IConfiguration config)
        {
            _connectionString = config.GetConnectionString("KeyConStr");
            ID_Receiver_Email_Notif = Convert.ToInt32(AppConfig.Configuration["AppSettings:ID_Receiver_Email_Notif"]);
            if (ID_Receiver_Email_Notif <= 0)
            {
                ID_Receiver_Email_Notif = 3;
            }
        }

        //private static string _connectionString = string.Empty;

        private string ConnectionString
        {
            get
            {
                var baseConn = string.IsNullOrEmpty(_connectionString)
                    ? GlobalFunction.ConnectionString
                    : _connectionString;
                return ConnectionStringWithOption(baseConn);
            }
        }
        private string ConnectionStringWithOption(string connectionString)
        {
            var builder = new MySqlConnectionStringBuilder(connectionString)
            {
                MinimumPoolSize = GlobalFunction.defConnStringOption.MinimumPoolSize,
                MaximumPoolSize = GlobalFunction.defConnStringOption.MaximumPoolSize,
                AllowLoadLocalInfile = GlobalFunction.defConnStringOption.AllowLoadLocalInfile,
                DefaultCommandTimeout = GlobalFunction.defConnStringOption.DefaultCommandTimeout,
                ConnectionTimeout = GlobalFunction.defConnStringOption.ConnectionTimeout,
                TreatTinyAsBoolean = GlobalFunction.defConnStringOption.TreatTinyAsBoolean,
                AllowZeroDateTime = GlobalFunction.defConnStringOption.AllowZeroDateTime,
                ConvertZeroDateTime = GlobalFunction.defConnStringOption.ConvertZeroDateTime,
                IgnoreCommandTransaction = GlobalFunction.defConnStringOption.IgnoreCommandTransaction,
                CharacterSet = GlobalFunction.defConnStringOption.CharacterSet,
            };
            builder.Pooling = true;
            builder.AllowUserVariables = true;
            builder.ConnectionReset = false;
            builder.ApplicationName = $"{GlobalFunction.AppName} {GlobalFunction.AppVersion}";

            return builder.ConnectionString;
        }

        public IDbConnection CreateConnection()
        {
            if (DBTrans != null && SharedConnection != null)
            {
                if (SharedConnection.State != ConnectionState.Open)
                    SharedConnection.Open();

                InitSession(SharedConnection);
                return SharedConnection;
            }
            else
            {
                var conn = new MySqlConnection(ConnectionString);
                conn.Open();
                InitSession(conn);
                return conn;
            }
        }

        public string DatabaseName()
        {
            if (SharedConnection != null)
                return SharedConnection.Database ?? "";

            var builder = new MySqlConnectionStringBuilder(ConnectionString);

            return !string.IsNullOrWhiteSpace(builder.Database) ? builder.Database : "poscabang";
        }

        private void InitSession(MySqlConnection conn)
        {
            int threadId = conn.ServerThread;
            if (_initializedSessions.ContainsKey(threadId))
                return; // sudah pernah di-init, skip

            conn.Execute($"SET SESSION sql_mode='{GlobalFunction.defSql_Mode}'");

            _initializedSessions[threadId] = true;
        }

        public T? ExecuteScalar<T>(string cmd, object? parameter = null)
        {
            var conn = CreateConnection();
            try
            {
                return conn.ExecuteScalar<T>(cmd, parameter, DBTrans, _CommandTimeout);
            }
            finally
            {
                if (DBTrans == null)
                    conn.Dispose();
            }
        }
        public async Task<T?> ExecuteScalarAsync<T>(string cmd, object? parameter = null)
        {
            return await RunWithTransactionLockAsync(async () =>
            {
                var conn = await GetConnectionAsync();
                try
                {
                    return await conn.ExecuteScalarAsync<T>(cmd, parameter, DBTrans, _CommandTimeout);
                }
                finally
                {
                    if (DBTrans == null)
                        await conn.DisposeAsync();
                }
            });
        }

        private async Task<T> RunWithTransactionLockAsync<T>(Func<Task<T>> action)
        {
            // Kalau sedang transaksi, kita lock supaya tidak overlap query
            if (DBTrans != null)
            {
                await _transactionLock.WaitAsync();
                try
                {
                    return await action();
                }
                finally
                {
                    _transactionLock.Release(); // Lepas lock setelah selesai
                }
            }
            else
            {
                // Kalau tidak dalam transaksi, langsung jalan paralel
                return await action();
            }
        }

        public List<T> FindList<T>(string cmd, object? parameter = null)
        {
            var conn = CreateConnection();
            try
            {
                var res = conn.Query<T>(cmd, parameter, DBTrans, commandTimeout: _CommandTimeout);
                return res.ToList();
            }
            finally
            {
                if (DBTrans == null)
                    conn.Dispose();
            }
        }

        public async Task<List<T>> FindListAsync<T>(string cmd, object? parameter = null)
        {
            return await RunWithTransactionLockAsync(async () =>
            {
                var conn = await GetConnectionAsync();
                try
                {
                    var res = await conn.QueryAsync<T>(cmd, parameter, DBTrans, _CommandTimeout);
                    return res.ToList();
                }
                finally
                {
                    if (DBTrans == null)
                        await conn.DisposeAsync();
                }
            });
        }

        private async Task InitSessionAsync(MySqlConnection conn)
        {
            int threadId = conn.ServerThread;
            if (_initializedSessions.ContainsKey(threadId))
                return; // sudah pernah di-init, skip

            await conn.ExecuteAsync($"SET SESSION sql_mode='{GlobalFunction.defSql_Mode}'");

            _initializedSessions[threadId] = true;
        }

        private async Task<MySqlConnection> GetConnectionAsync()
        {
            if (DBTrans != null && SharedConnection != null)
            {
                while (SharedConnection.State == ConnectionState.Connecting)
                {
                    await Task.Delay(100); // Tunggu sampai koneksi selesai membuka
                }
                if (SharedConnection.State != ConnectionState.Open)
                    await SharedConnection.OpenAsync();

                await InitSessionAsync(SharedConnection);
                return SharedConnection;
            }
            else
            {
                var conn = new MySqlConnection(ConnectionString);
                await conn.OpenAsync();
                await InitSessionAsync(conn);
                return conn;
            }
        }
    }
}
