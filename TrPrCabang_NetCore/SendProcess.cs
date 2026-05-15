using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrPrCabang_NetCore.Connection;
using TrPrCabang_NetCore.Controllers;
using TrPrCabang_NetCore.DataAccess;
using TrPrCabang_NetCore.Models;

namespace TrPrCabang_NetCore
{
    public class SendProcess
    {
        private readonly Utility ObjUtil;
        private readonly CompressHelper _comp;
        private readonly TrPrDA _trDA;
        private readonly MasterDA _masDA;
        private readonly ConfigDA _coDA;
        private readonly ServiceController _service;
        private readonly IDbServices db;

        private MySqlConnection ConnTrPr = new();
        public Config conf;
        private string Jobs = "SEND2";
        private int MaxThread = 1;
        private int CountThread = 0;
        private readonly List<string> _tokoSend = new();
        private readonly object Lock = new();

        private string EmailTo = string.Empty;
        private string EmailCC = string.Empty;

        public SendProcess(IDbServices db, Utility util, CompressHelper comp, TrPrDA trDA, MasterDA masDA, ConfigDA coDA, ServiceController service, Config conf)
        {
            this.db = db;
            ObjUtil = util;
            _comp = comp;
            _trDA = trDA;
            _masDA = masDA;
            _coDA = coDA;
            _service = service;

        }

        public async Task Run()
        {
            try
            {
                using var dtService = db.CreateConnection();

                Log($"DB OK | Mode: {Jobs}");

                ObjUtil.Tracelog(Jobs, "Application UP.", Utility.TipeLog.Debug);
                await _trDA.InitialTables(Jobs);

                ObjUtil.Tracelog(Jobs,
                    $"Command Arguments: {Jobs}",
                    Utility.TipeLog.Debug);

                // Config
                string infoConfig = string.Empty;
                conf = _coDA.GetConfig(Jobs, infoConfig);

                if (!string.IsNullOrWhiteSpace(infoConfig))
                {
                    ObjUtil.Tracelog(Jobs, infoConfig, Utility.TipeLog.Error);
                    Exit(infoConfig);
                }

                ObjUtil.Tracelog(Jobs,
                    $"Initial Config Program [DETAIL]\r\n" +
                    $"KodeCabang             : {conf.KodeCabang}\r\n" +
                    $"API SD4 Status PAJAK   : {conf.UrlStatusPajak}\r\n" +
                    $"API SD4 Master PAJAK   : {conf.UrlMasterPajak}\r\n" +
                    $"API SD4 Status SUPPLIER: {conf.UrlStatusSupplier}\r\n" +
                    $"API SD4 Master SUPPLIER: {conf.UrlMasterSupplier}\r\n" +
                    $"API POSRT Cabang       : {conf.UrlPosRT_Service}\r\n" +
                    $"TimeoutService         : {conf.TimeoutService}",
                    Utility.TipeLog.Debug);

                await RunSend2Async();
            }
            catch (Exception ex)
            {
                LogError($"{ex.Message}\n{ex.StackTrace}");

                ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error);
                ObjUtil.SendEmail(EmailTo, EmailCC, $"[ERROR] TrPrCabang - {conf.KodeCabang}", $"Proses TrPr Cabang ({conf.KodeCabang}) terhenti:\n{ex.Message}\n{ex.StackTrace}");

                Environment.Exit(1);
            }
        }

        // ── SEND2 ─────────────────────────────────────────────────────────────────
        public async Task RunSend2Async()
        {
            MaxThread = conf.MaxThread;
            ObjUtil.Tracelog(Jobs, $"THREAD {Jobs} : {MaxThread}", Utility.TipeLog.Debug);

            List<string> dtTokoLog = await _trDA.GetListKodeTokoByLogMaster(Jobs);
            Log($"[SEND2] Total toko: {dtTokoLog.Count} | Thread: {MaxThread}");

            if (dtTokoLog.Count == 0) { Log("[SEND2] Tidak ada toko."); return; }

            BuildTokoArrays(dtTokoLog);

            var tasks = new List<Task>();
            for (int i = 0; i < 5; i++)
            {
                if (string.IsNullOrWhiteSpace(_tokoSend[i])) continue;
                int idx = i;
                string tokoList = _tokoSend[i];
                Log($"[THREAD {idx + 1}] Count: {tokoList.Split(',').Length} | List: {tokoList}");
                lock (Lock) CountThread++;
                tasks.Add(Task.Run(async () => await Send2DoWorkAsync(idx, tokoList)));
                await Task.Delay(1000);
            }

            await Task.WhenAll(tasks);
            Send2Complete();
        }

        // ── SendDoWork ────────────────────────────────────────────────────────────
        public async Task Send2DoWorkAsync(int threadIdx, string listToko)
        {
            string dateNow = DateTime.Now.ToString("yyMMddHHmm");

            try
            {
                using var conn = db.CreateConnection();

                ObjUtil.Tracelog(Jobs, $"THREAD {threadIdx} START", Utility.TipeLog.Debug);

                // ── TKX & DCX ─────────────────────────────
                Log($"THREAD {threadIdx}|[PARTISIPAN] Draft TKX & DCX...");

                var draftList = await _masDA.GetPartisipanLogMaster_TKX_DCX(Jobs, listToko);

                Log($"THREAD {threadIdx}|Rows: {draftList.Count}");

                int iProg = 0;

                foreach (var item in draftList)
                {
                    try
                    {
                        string? kodeToko = item.KODETOKO;
                        string? tglStatus = item.TGL_STATUS;
                        string? tglProses = item.TGL_PROSES;

                        bool sendTKX = item.JENIS_TKX != null;
                        bool sendDCX = item.JENIS_DCX != null;

                        string sqlTKX = string.Empty;
                        string sqlDCX = string.Empty;

                        if (sendTKX)
                        {
                            sqlTKX = await _masDA.CreateQueryTKX_ByTOKO(Jobs, kodeToko, tglProses, sendTKX);

                            if (!sendTKX)
                            {
                                await _trDA.UpdateLogMaster(
                                    Jobs, "TKX",
                                    tglStatus, tglProses, kodeToko,
                                    0, "", $"NOK|T:{threadIdx}");
                            }
                        }

                        if (sendDCX)
                        {
                            sqlDCX = await _masDA.CreateQueryDCX_ByTOKO(Jobs, kodeToko, tglProses, sendDCX);

                            if (!sendDCX)
                            {
                                await _trDA.UpdateLogMaster(
                                    Jobs, "DCX",
                                    tglStatus, tglProses, kodeToko,
                                    0, "", $"NOK|T:{threadIdx}");
                            }
                        }

                        string sqlTask = $"{sqlTKX}\n{sqlDCX}";

                        if (!string.IsNullOrWhiteSpace(sqlTask))
                        {
                            string taskName = BuildTaskName(
                                new[] { (sendTKX, "TKX"), (sendDCX, "DCX") },
                                kodeToko,
                                dateNow);

                            bool ok = await SendToServerAsync(Jobs, sqlTask, kodeToko.ToUpper(), taskName);

                            if (ok)
                            {
                                if (sendTKX)
                                {
                                    await _trDA.UpdateLogMaster(
                                        Jobs, "TKX",
                                        tglStatus, tglProses, kodeToko,
                                        0, "", $"OK|T:{threadIdx}");
                                }

                                if (sendDCX)
                                {
                                    await _trDA.UpdateLogMaster(
                                        Jobs, "DCX",
                                        tglStatus, tglProses, kodeToko,
                                        0, "", $"OK|T:{threadIdx}");
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        ObjUtil.Tracelog(Jobs,
                            $"THREAD {threadIdx}|{ex.Message}",
                            Utility.TipeLog.Error);
                    }

                    int percent = draftList.Count > 0
                        ? (int)((double)iProg / draftList.Count * 100)
                        : 100;

                    Log($"THREAD {threadIdx}|[{percent}%] TKX/DCX [{item.KODETOKO}]");

                    iProg++;

                    await Task.Delay(50);
                }

                Log($"THREAD {threadIdx}|FINISH");

                ObjUtil.Tracelog(Jobs,
                    $"THREAD {threadIdx} FINISH",
                    Utility.TipeLog.Debug);
            }
            catch (Exception ex)
            {
                using var conn = db.CreateConnection();

                ObjUtil.Tracelog(Jobs,
                    $"THREAD {threadIdx}|{ex.Message}\n{ex.StackTrace}",
                    Utility.TipeLog.Error);

                ObjUtil.SendEmail(EmailTo, EmailCC, $"[ERROR] SEND2 - {conf.KodeCabang}", $"THREAD {threadIdx} ERROR:\n{ex.Message}\n{ex.StackTrace}");
            }
            finally
            {
                lock (Lock)
                {
                    CountThread--;
                }
            }
        }

        public async Task Send2Complete()
        {
            await _trDA.UpdateConst(Jobs, "TRPSEND2", "TrPrCabang", DateTime.Now, DateTime.Now);
            CloseConn(ConnTrPr);
            Log("SEND2 Finish.");
            Thread.Sleep(3000);
        }

        // ── Helpers ───────────────────────────────────────────────────────────────

        // Kirim ke server POSRT (REST atau SOAP)
        //static bool SendToServer(string jobs, string sql, string kodeToko, string taskName, MySqlConnection conn)
        //{
        //    string msgService = "";
        //    if (Conf.IsPosRT_RestApi)
        //        return Service.SendTrPrTask_RestApi(jobs, sql, kodeToko, taskName, conf, ref msgService);

        //    byte[] sqlByte = ObjComp.Compress(sql);
        //    return Service.SendTrPrTask_Service(jobs, sqlByte, kodeToko, taskName, Conf, ref msgService);
        //}


        public async Task<bool> SendToServerAsync(string jobs, string sql, string kodeToko, string taskName)
        {
            try
            {
                if (conf.IsPosRT_RestApi)
                {
                    return await _service.SendTrPrTask_RestApi(jobs, sql, kodeToko, taskName, conf, null);
                }

                byte[] sqlByte = _comp.Compress(sql);

                return await _service.SendTrPrTask_Service(jobs, sqlByte, kodeToko, taskName, conf, null);
            }
            catch (Exception ex)
            {
                // kalau butuh logging DB → buat connection di sini via db
                using var conn = db.CreateConnection();

                ObjUtil.Tracelog(jobs,
                    $"SendToServer ERROR | {kodeToko} | {ex.Message}",
                    Utility.TipeLog.Error);

                return false;
            }
        }

        // Build task name dari list (flag, nama)
        static string BuildTaskName(IEnumerable<(bool send, string name)> items, string kodeToko, string dateNow)
        {
            var parts = new List<string>();
            foreach (var (send, name) in items)
                if (send) parts.Add(name);
            return $"{string.Join("_", parts)}|{kodeToko.ToUpper()}|{dateNow}";
        }

        // Tambah row ke DtQueryMaster
        static void AddQueryRow(DataTable dt, string jenis, string tglProses, string query)
        {
            var row = dt.NewRow();
            row["JENIS"] = jenis;
            row["TGL_PROSES"] = tglProses;
            row["QUERY"] = query;
            dt.Rows.Add(row);
        }

        private void BuildTokoArrays(List<string> tokoList)
        {
            int maxPerThread = tokoList.Count / MaxThread;
            int tIdx = 0, tCount = 0;
            string list = string.Empty;

            foreach (string kode in tokoList)
            {
                list += $"'{kode.ToUpper()}',";
                if (++tCount == maxPerThread)
                {
                    _tokoSend[tIdx] = list.TrimEnd(',');
                    Log($"ArrayToko[{tIdx}] Ready ({tCount} toko)");
                    list = string.Empty; tCount = 0;
                    if (tIdx < MaxThread - 1) tIdx++;
                }
            }
            if (!string.IsNullOrWhiteSpace(list))
            {
                string rem = list.TrimEnd(',');
                _tokoSend[tIdx] = string.IsNullOrWhiteSpace(_tokoSend[tIdx]) ? rem : $"{_tokoSend[tIdx]},{rem}";
            }
        }

        static string GetStr(DataRow dr, string col) =>
            dr[col] is DBNull || dr[col] == null ? string.Empty : dr[col].ToString()!;

        static bool IsNullOrDbNull(DataRow dr, string col) =>
            dr[col] == null || dr[col] is DBNull;

        static void CloseConn(MySqlConnection conn)
        {
            try { if (conn?.State != ConnectionState.Closed) conn?.Close(); } catch { }
        }

        private string Exit(string msg)
        {
            LogError(msg);
            Thread.Sleep(2000);
            Environment.Exit(1);
            return string.Empty;
        }

        private void Log(string msg)
        {
            lock (Lock) Console.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {msg}");
        }

        private void LogError(string msg)
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
