using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Connection
{
    public class GlobalFunction
    {
        private static string _connectionString = string.Empty;
        public static string? AppInitial { get; set; } = AppConfig.Configuration["MyAppSettings:AppInitial"];
        public static string? AppName { get; set; } = AppConfig.Configuration["MyAppSettings:AppName"];
        public static string? AppVersion { get; set; } = AppConfig.Configuration["MyAppSettings:Version"];
        public static string defSql_Mode { get; set; } = "STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
        public static MySqlConnector.MySqlConnectionStringBuilder defConnStringOption { get; set; } = new MySqlConnector.MySqlConnectionStringBuilder()
        {
            MinimumPoolSize = 0,
            MaximumPoolSize = 100,
            AllowLoadLocalInfile = true,
            DefaultCommandTimeout = 0,
            ConnectionTimeout = 30,
            TreatTinyAsBoolean = false,
            AllowZeroDateTime = true,
            ConvertZeroDateTime = true,
            IgnoreCommandTransaction = true,
            CharacterSet = "latin1",

            //ini hardcode dan gabisa diubah di appsettings.json
            Pooling = true,
            AllowUserVariables = true,
            ConnectionReset = false,
            ApplicationName = $"{GlobalFunction.AppName} {GlobalFunction.AppVersion}",
        };

        public static string ConnectionString
        {
            get
            {
                return _connectionString.Replace("[pwd]", "c@B4n9@wRc$d7");
            }
            set
            {
                _connectionString = value;
            }
        }
    }
}
