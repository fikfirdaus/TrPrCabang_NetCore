using MySqlConnector;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrPrCabang_NetCore.Controllers;
using TrPrCabang_NetCore.DataAccess;
using TrPrCabang_NetCore.Models;
using TrPrCabang_NetCore.Connection;

namespace TrPrCabang_NetCore
{
    public class TokoProcess
    {
        static readonly Utility ObjUtil;
        static readonly TrPrDA ObjTrDA;
        static readonly ServiceController ObjSvr;
        private readonly IDbServices db;

        static MySqlConnection ConnTrPr;
        static readonly object Lock;

        public TokoProcess(IDbServices db)
        {
            this.db = db;
        }

        public async Task Run()
        {
            try
            {
                Log("Loading ...");
                var frm = new TokoProcess(db);
                await frm.SyncTokoExtendedAsync();
            }
            catch (Exception ex)
            {
                ObjUtil.TraceLogFile($"Error FrmToko: {ex.Message}\n{ex.StackTrace}");
                LogError(ex.Message);
                Environment.Exit(1);
            }
        }
       

        // ── Sync Master Toko Extended ─────────────────────────────────────────────
        public async Task SyncTokoExtendedAsync()
        {
            await Task.Run(async () =>
            {
                var lstTokoExtended = new List<TOKO_EXTENDED>();
                int iCab = 0;

                try
                {
                    // Koneksi DB
                    using var dtService = db.CreateConnection();

                    // Get List Cabang
                    List<string> dtCabang = await ObjTrDA.GetCabang("TOK");
                    Log($"Total Cabang: {dtCabang.Count}");

                    foreach (string dr in dtCabang)
                    {
                        int percent = dtCabang.Count > 0 ? (int)((double)iCab / dtCabang.Count * 100) : 0;
                        string kdCabang = dr;

                        Log($"WS Toko_Extended {kdCabang} [GET] ({percent}%)");

                        var responWS = await ObjSvr.GetTblMasterTokoService("TOKO_EXTENDED", kdCabang);
                        if (responWS.ERR_CODE == "00")
                        {
                            if (responWS.DETAIL != null && responWS.DETAIL is JArray arr)
                            {
                                foreach (JObject item in arr)
                                {
                                    lstTokoExtended.Add(
                                        JsonConvert.DeserializeObject<TOKO_EXTENDED>(item.ToString())!
                                    );
                                }
                            }
                            Log($"WS Toko_Extended {kdCabang} [DONE] ({percent}%)");
                        }
                        else
                        {
                            ObjUtil.Tracelog("TOK",
                                $"Get Master TOKO_EXTENDED | {kdCabang}\n" +
                                $"Error API: {responWS.ERR_MSG}\n" +
                                $"JSON: {responWS.DETAIL}",
                                Utility.TipeLog.Debug);
                            LogError($"WS Toko_Extended {kdCabang} [ERROR]");
                            break;
                        }

                        iCab++;
                    }

                    Log($"Toko_Extended Count: {lstTokoExtended.Count}");
                    ObjUtil.Tracelog("TOK", $"Get Toko Extended | Count: {lstTokoExtended.Count}", Utility.TipeLog.Debug);

                    if (lstTokoExtended.Count > 0)
                    {
                        Log("Toko_Extended Insert Data [LOADING]");
                        await ObjTrDA.InsertMasterTokoExtended(lstTokoExtended);
                        Log("Toko_Extended Insert Data [DONE]");
                    }
                }
                catch (Exception ex)
                {
                    ObjUtil.Tracelog("TOK", $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error);
                    LogError(ex.Message);
                }
                finally
                {
                    try { if (ConnTrPr?.State != ConnectionState.Closed) ConnTrPr?.Close(); } catch { }
                }
            });

            Thread.Sleep(3000);
            Log("Selesai.");
            Environment.Exit(0);
        }

        // ── Console Output ────────────────────────────────────────────────────────
        static void Log(string msg)
        {
            lock (Lock) Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {msg}");
        }

        static void LogError(string msg)
        {
            lock (Lock)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] [ERROR] {msg}");
                Console.ResetColor();
            }
        }
    }
}
