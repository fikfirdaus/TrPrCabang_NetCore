using TrPrCabang_NetCore;
using TrPrCabang_NetCore.Connection;
using Dapper;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.VisualBasic;
using System;
using System.Collections;
using System.Data;
using System.Diagnostics;
using TrPrCabang;
using TrPrCabang_NetCore.Controllers;
using TrPrCabang_NetCore.DataAccess;
using TrPrCabang_NetCore.Models;
class Program
{
    public readonly IDbServices? db;

    static async Task Main(string[] args)
    {
        var db = AppConfig.DbService;
        var objUtil = new Utility(db);
        var compress = new CompressHelper(objUtil);
        var httpClient = new HttpClient();
        var trprDA = new TrPrDA(db);
        var masterDA = new MasterDA(db);
        var configDA = new ConfigDA(db);
        var service = new ServiceController(httpClient, objUtil, compress);
        var conf = new Config();

        try
        {
            objUtil.Tracelog("Main()", $"Mulai program : {DateTime.Now:dd MMM yyyy HH:mm:ss}", Utility.TipeLog.Info);
            Console.WriteLine($"Mulai program : {DateTime.Now:dd MMM yyyy HH:mm:ss}");
            string arg = args.Length > 1 ? args[1].Trim().ToUpper() : string.Empty;

            switch (arg)
            {
                case "G":
                case "S":
                case "G2":
                    new MainProcess();
                    break;
                case "S2":
                    await new SendProcess(db, objUtil, compress, trprDA, masterDA, configDA, service, conf).Run();
                    break;
                case "TOK":
                    await new TokoProcess(db).Run();
                    break;

                default:
                    new MainProcess();
                    break;
            }

            objUtil.Tracelog("Main()", $"Selesai program : {DateTime.Now:dd MMM yyyy HH:mm:ss}", Utility.TipeLog.Info);
            Console.WriteLine($"Selesai program : {DateTime.Now:dd MMM yyyy HH:mm:ss}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error : {ex.Message}");
            objUtil.Tracelog("Main()", $"Error : {ex.Message} \n {ex.StackTrace}", Utility.TipeLog.Error);
        }
        objUtil.Tracelog("Main()", $"Selesai program : {DateTime.Now:dd MMM yyyy HH:mm:ss}", Utility.TipeLog.Info);
    }
}