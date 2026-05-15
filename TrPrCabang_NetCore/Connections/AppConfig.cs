using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System.Runtime.InteropServices;

namespace TrPrCabang_NetCore.Connection
{
    public static class AppConfig
    {
        public static IHost? Host { get; private set; }
        public static IConfiguration Configuration => Host?.Services.GetRequiredService<IConfiguration>() ??
                                                      throw new InvalidOperationException("Host is not initialized.");

        public static IDbServices DbService => Host?.Services.GetRequiredService<IDbServices>() ??
                                               throw new InvalidOperationException("Host is not initialized.");

        public static void Initialize(string[] args)
        {
            var currentDir = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, @"..\..\.."));

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Host = Microsoft.Extensions.Hosting.Host.CreateDefaultBuilder(args)
                    .ConfigureAppConfiguration(config =>
                    {
                        config.SetBasePath(currentDir)
                              .AddJsonFile("buatfile_netcore.json", optional: false, reloadOnChange: true);
                    })
                    .ConfigureServices(services =>
                    {
                        services.AddSingleton<IDbServices, DbService>();
                    })
                    .Build();
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                Host = Microsoft.Extensions.Hosting.Host
                     .CreateDefaultBuilder(args)
                     .ConfigureAppConfiguration(config =>
                     {
                         config.SetBasePath(AppContext.BaseDirectory)
                               .AddJsonFile("buatfile_netcore.json", optional: false, reloadOnChange: true);
                     })
                     .ConfigureServices(services =>
                     {
                         services.AddSingleton<IDbServices, DbService>();
                     })
                     .Build();
            }

        }
    }
}
