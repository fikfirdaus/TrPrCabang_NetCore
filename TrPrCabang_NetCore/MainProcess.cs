using System;
using System.Collections.Generic;
using System.Data;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;
using Newtonsoft.Json;
using TrPrCabang_NetCore;
using TrPrCabang_NetCore.Controllers;
using TrPrCabang_NetCore.DataAccess;
using TrPrCabang_NetCore.Models;

namespace TrPrCabang
{
    public class MainProcess
    {
        private static readonly Utility ObjUtil;
        private static readonly TrPrDA TrDA;
        private static readonly MasterDA MasDA;
        private static readonly PosRTDA PoDA;
        private static readonly ConfigDA CoDA;
        private static readonly ServiceController Service;

        private static MySqlConnection ConnTrPr = new();
        private static readonly string[] ArrayTokoSend = new string[5];
        private static Config Conf = new();
        private static string Jobs = "";
        private static int MaxThread = 1;
        private static int CountThread = 0;
        private static string _emailTo = string.Empty;
        private static string _emailCC = string.Empty;

        public async Task RunAsync(string[] args)
        {
            try
            {
                Console.WriteLine($"{AppDomain.CurrentDomain.FriendlyName}");

                // Cek Command Arguments
                if (args.Length > 0)
                {
                    Jobs = args[0].ToUpper() switch
                    {
                        "G" => "GET",
                        "S" => "SEND",
                        "G2" => "GET2",
                        _ => ""
                    };
                }

                if (string.IsNullOrWhiteSpace(Jobs))
                {
                    ObjUtil.TraceLogFile("Command Arguments Kosong.");
                    Thread.Sleep(3000);
                    return;
                }


                ObjUtil.Tracelog(Jobs, $"{AppDomain.CurrentDomain.FriendlyName} UP.", Utility.TipeLog.Debug);

                // Initial Table Database
                Log("InitialTables");
                await TrDA.InitialTables(Jobs);
                ObjUtil.Tracelog(Jobs, "Initial Tables Program [CHECK].", Utility.TipeLog.Debug);
                ObjUtil.Tracelog(Jobs, $"Command Arguments: {Jobs}", Utility.TipeLog.Debug);

                // Initial Config
                string infoConfig = "";
                ObjUtil.Tracelog(Jobs, "Initial Config Program [CHECK].", Utility.TipeLog.Debug);
                Conf = CoDA.GetConfig(Jobs, infoConfig);

                if (!string.IsNullOrWhiteSpace(infoConfig))
                {
                    ObjUtil.Tracelog(Jobs, infoConfig, Utility.TipeLog.Error);
                    return;
                }

                ObjUtil.Tracelog(Jobs, $"Initial Config Program [DETAIL]\n" +
                    $"KodeCabang: {Conf.KodeCabang}\n" +
                    $"API SD4 Status PAJAK: {Conf.UrlStatusPajak}\n" +
                    $"API SD4 Master PAJAK: {Conf.UrlMasterPajak}\n" +
                    $"API SD4 Status SUPPLIER: {Conf.UrlStatusSupplier}\n" +
                    $"API SD4 Master SUPPLIER: {Conf.UrlMasterSupplier}\n" +
                    $"API SD4 Partisipan SUPPLIER: {Conf.UrlPartisipanSupplier}\n" +
                    $"API SD4 Status PROMOSI: {Conf.UrlStatusPromo}\n" +
                    $"API SD4 Master PROMOSI: {Conf.UrlMasterPromo}\n" +
                    $"API SD4 Partisipan PROMOSI: {Conf.UrlPartisipanPromo}\n" +
                    $"Send API REST POSRT Cabang: {Conf.IsPosRT_RestApi}\n" +
                    $"API POSRT Cabang: {Conf.UrlPosRT_Service}\n" +
                    $"API POSRT REST Cabang: {Conf.UrlPosRT_RestApi}\n" +
                    $"Auth Username API POSRT REST Cabang: {Conf.AuthUserPosRT_RestApi}\n" +
                    $"Auth Password API POSRT REST Cabang: {Conf.AuthPassPosRT_RestApi}\n" +
                    $"TimeoutService: {Conf.TimeoutService}",
                    Utility.TipeLog.Debug);

                if (Jobs == "GET")
                {
                    await Task.Run(() => GetDoWork());
                    await GetComplete();
                }
                else if (Jobs == "GET2")
                {
                    await Task.Run(() => GetMaster2DoWork());
                    GetMaster2Complete();
                }
                else if (Jobs == "SEND")
                {
                    MaxThread = Conf.MaxThread;
                    ObjUtil.Tracelog(Jobs, $"THREAD {Jobs} : {MaxThread}", Utility.TipeLog.Debug);

                    // Get List Toko dari log_master
                    List<string> listKodeToko = await TrDA.GetListKodeTokoByLogMaster(Jobs);
                    ObjUtil.Tracelog(Jobs, $"Get List Toko [log_master] : {listKodeToko.Count}", Utility.TipeLog.Debug);

                    if (listKodeToko.Count == 0)
                    {
                        Log("Tidak ada data toko untuk diproses.");
                        return;
                    }

                    // Split List Toko sesuai maksimal setting jumlah thread
                    int countLoopToko = 0;
                    string listToko = "";
                    int iThread = 0;
                    int countThread = 0;
                    int maxTokoPerThread = listKodeToko.Count / MaxThread;

                    foreach (var dr in listKodeToko)
                    {
                        listToko += $"'{dr.ToUpper()}',";
                        countThread++;
                        countLoopToko++;

                        if (countThread == maxTokoPerThread)
                        {
                            listToko = listToko.TrimEnd(',');
                            ArrayTokoSend[iThread] = listToko;
                            Log($"ArrayToko[{iThread}] Ready");
                            listToko = "";
                            countThread = 0;
                            if (iThread < MaxThread - 1) iThread++;
                        }
                    }

                    // Sisa toko masuk ke array index terakhir
                    if (!string.IsNullOrWhiteSpace(listToko))
                    {
                        listToko = listToko.TrimEnd(',');
                        ArrayTokoSend[iThread] = string.IsNullOrWhiteSpace(ArrayTokoSend[iThread])
                            ? listToko
                            : ArrayTokoSend[iThread] + "," + listToko;
                    }

                    // Run Tasks SEND
                    var sendTasks = new List<Task>();
                    CountThread = 0;

                    for (int idx = 0; idx < 5; idx++)
                    {
                        if (!string.IsNullOrWhiteSpace(ArrayTokoSend[idx]))
                        {
                            int threadIdx = idx;
                            string listTokoThread = ArrayTokoSend[idx];
                            Log($"THREAD {threadIdx + 1} [{listTokoThread.Split(',').Length}] Starting...");
                            CountThread++;
                            sendTasks.Add(Task.Run(() => SendDoWork(threadIdx, listTokoThread)));
                            Thread.Sleep(1000);
                        }
                    }

                    await Task.WhenAll(sendTasks);
                    SendComplete();
                }
            }
            catch (Exception ex)
            {
                ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error);
                ObjUtil.SendEmail(_emailTo, _emailCC,
                    $"[ERROR] TrPrCabang - {Conf.KodeCabang}",
                    $"Proses TrPr Cabang ({Conf.KodeCabang}) terhenti:\n{ex.Message}\n{ex.StackTrace}");
            }
        }

        // =====================================================================
        // GET
        // =====================================================================
        public async Task GetDoWork()
        {
            string result = "";
            DateTime tglStatus = default;
            DateTime tglProses = default;

            try
            {
                // ==================== PROMOSI ====================
                try
                {
                    bool isGetMasterPromo = false;
                    int totalRowsMasterPromo = 0;
                    string msgMasterPromo = "";
                    tglStatus = default;
                    tglProses = default;

                    ObjUtil.Tracelog(Jobs, "[STATUS] Get PROMOSI - START.", Utility.TipeLog.Debug);
                    Log("[STATUS] Get PROMOSI ...");
                    result = await Service.GetStatusService(Jobs, Conf, "HO", "PROMOSI", true);
                    var objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);
                    ObjUtil.Tracelog(Jobs, "[STATUS] Get PROMOSI - FINISH.", Utility.TipeLog.Debug);

                    if (objStatus != null)
                    {
                        foreach (var objSt in objStatus)
                        {
                            Log($"[STATUS] Result PROMOSI : {objSt.FLAG_PROSES}-{objSt.JML_RECORD}");
                            ObjUtil.Tracelog(Jobs, $"[STATUS] Result PROMOSI : {objSt.FLAG_PROSES}-{objSt.JML_RECORD}\nTGL_STATUS:{objSt.TGL_STATUS}\nTGL_PROSES:{objSt.TGL_PROSES}", Utility.TipeLog.Debug);

                            try { tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }
                            try { tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }

                            totalRowsMasterPromo += objSt.JML_RECORD;

                            if (!await TrDA.CekStatusLogMaster(Jobs, "PROMO", tglProses))
                            {
                                if (objSt.FLAG_PROSES == "Y") { isGetMasterPromo = true; break; }
                                else ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[PROMO] FLAG_PROSES : {objSt.FLAG_PROSES}\nJSON : {result}");
                            }
                            else
                            {
                                msgMasterPromo += $"Data PROMO dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.\n";
                            }
                        }

                        if (isGetMasterPromo)
                        {
                            bool getMasterPromo = true;
                            Log("[MASTER] Get PROMOSI ...");
                            result = await Service.GetMasterPromosiService(Jobs, Conf);
                            var objPromo = JsonConvert.DeserializeObject<List<PROMO>>(result);
                            ObjUtil.Tracelog(Jobs, "[MASTER] Get PROMOSI - FINISH.", Utility.TipeLog.Debug);
                            Log($"[MASTER] Result PROMOSI : {objPromo?.Count}-{totalRowsMasterPromo}");

                            if (objPromo?.Count == totalRowsMasterPromo)
                            {
                                Log("[MASTER] Insert PROMOSI ...");
                                if (await TrDA.InsertTrPrPromosi(Jobs, objPromo))
                                {
                                    var dtPromoGroupID = TrDA.GetListPromoGroupID(Jobs);
                                    getMasterPromo = true;
                                }
                                else
                                {
                                    ObjUtil.TraceLogFile($"ERROR|InsertTrPrPromosi:\nJSON: {result}");
                                    getMasterPromo = false;
                                }
                            }
                            else
                            {
                                ObjUtil.Tracelog(Jobs, $"[PROMO] Jumlah baris tidak sesuai.\nSTATUS: {totalRowsMasterPromo}\nMASTER: {objPromo?.Count}", Utility.TipeLog.Warning);
                                ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[PROMO] Jumlah baris tidak sesuai.\nSTATUS: {totalRowsMasterPromo}\nMASTER: {objPromo?.Count}");
                                getMasterPromo = false;
                            }

                            if (getMasterPromo)
                            {
                                msgMasterPromo = "";
                                bool isGetPartisipanPromo = false;
                                int totalRowsPartisipanPromo = 0;

                                Log("[STATUS] Get PARTISIPAN ...");
                                result = await Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "PARTISIPAN", true);
                                objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);

                                foreach (var objSt in objStatus!)
                                {
                                    Log($"[STATUS] Result PARTISIPAN : {objSt.FLAG_PROSES}-{objSt.JML_RECORD}");
                                    try { tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }
                                    try { tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }
                                    totalRowsPartisipanPromo += objSt.JML_RECORD;
                                    if (objSt.FLAG_PROSES == "Y") { isGetPartisipanPromo = true; break; }
                                    else msgMasterPromo += $"Data PARTISIPAN dengan TGL_PROSES: {tglProses:yyyy-MM-dd}, FLAG_PROSES: {objSt.FLAG_PROSES}.\n";
                                }

                                if (isGetPartisipanPromo)
                                {
                                    Log($"[PARTISIPAN PROMO] Get Data {Conf.KodeCabang} ...");
                                    result = await Service.GetPartisipanService(Jobs, Conf, Conf.KodeCabang, "PROMO");
                                    var objPartisipanPromo = JsonConvert.DeserializeObject<List<PARTISIPAN_PROMO>>(result);
                                    Log($"[PARTISIPAN PROMO] Result : {objPartisipanPromo?.Count}-{objStatus[0].JML_RECORD}");

                                    if (objPartisipanPromo?.Count == totalRowsPartisipanPromo)
                                    {
                                        Log("[PARTISIPAN] Insert PARTISIPAN PROMOSI ...");
                                        if (await TrDA.InsertPartisipanPromosi(Jobs, objPartisipanPromo, tglStatus.ToString("yyyy-MM-dd"), tglProses.ToString("yyyy-MM-dd HH:mm:ss")))
                                        {
                                            Log("[LOG] Insert LOG PROMOSI ...");
                                            TrDA.InsertLogMaster(Jobs, "PROMO", tglStatus.ToString("yyyy-MM-dd"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"));
                                            TrDA.CheckValidMasterAndPartisipanTrPr(Jobs, Conf.KodeCabang);
                                        }
                                    }
                                    else
                                    {
                                        ObjUtil.Tracelog(Jobs, $"[PROMO] Jumlah baris PARTISIPAN tidak sesuai.\nSTATUS: {totalRowsPartisipanPromo}\nPARTISIPAN: {objPartisipanPromo?.Count}", Utility.TipeLog.Warning);
                                        ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[PROMO] Jumlah baris PARTISIPAN tidak sesuai.\nSTATUS: {totalRowsPartisipanPromo}\nPARTISIPAN: {objPartisipanPromo?.Count}");
                                    }
                                }
                            }
                        }
                        else
                        {
                            ObjUtil.Tracelog(Jobs, msgMasterPromo, Utility.TipeLog.Error);
                        }
                    }
                }
                catch (Exception ex) { ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error); }

                // ==================== SUPPLIER ====================
                try
                {
                    tglStatus = default; tglProses = default;
                    Log("[STATUS] Get SUPPLIER ...");
                    result = await Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "SUPPLIER", true);
                    var objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);

                    if (objStatus != null)
                    {
                        foreach (var sta in objStatus)
                        {
                            if (sta.JENIS.ToUpper().Contains("MASTER"))
                            {
                                try { tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }
                                try { tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd HH:mm:ss", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }
                            }
                        }

                        if (!await TrDA.CekStatusLogMaster(Jobs, "SUPPLIER", tglProses))
                        {
                            bool getMasterSupplier = false;
                            foreach (var sta in objStatus)
                            {
                                if (sta.JENIS.ToUpper().Contains("MASTER") && sta.FLAG_PROSES == "Y")
                                {
                                    Log("[MASTER] Get SUPPLIER ...");
                                    result = await Service.GetMasterSupplierService(Jobs, Conf);
                                    var objSupplier = JsonConvert.DeserializeObject<List<SUPPLIER>>(result);
                                    Log($"[MASTER] Result SUPPLIER : {objSupplier?.Count}-{sta.JML_RECORD}");

                                    if (objSupplier?.Count == sta.JML_RECORD)
                                    {
                                        Log("[MASTER] Insert SUPPLIER ...");
                                        TrDA.InsertMasterSupplier(Jobs, objSupplier);
                                        getMasterSupplier = true;
                                    }
                                    else
                                    {
                                        ObjUtil.Tracelog(Jobs, $"[SUPPLIER] Jumlah baris tidak sesuai.\nSTATUS: {sta.JML_RECORD}\nMASTER: {objSupplier?.Count}", Utility.TipeLog.Warning);
                                        ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[SUPPLIER] Jumlah baris tidak sesuai.\nSTATUS: {sta.JML_RECORD}\nMASTER: {objSupplier?.Count}");
                                    }
                                }

                                if (getMasterSupplier && sta.JENIS.ToUpper().Contains("PARTISIPAN") && sta.FLAG_PROSES == "Y")
                                {
                                    Log($"[PARTISIPAN SUPPLIER] Get Data {Conf.KodeCabang} ...");
                                    result = await Service.GetPartisipanService(Jobs, Conf, Conf.KodeCabang, "SUPPLIER");
                                    var objPartisipanSupplier = JsonConvert.DeserializeObject<List<PARTISIPAN_SUPPLIER>>(result);
                                    Log($"[PARTISIPAN SUPPLIER] Result : {objPartisipanSupplier?.Count}-{sta.JML_RECORD}");

                                    if (objPartisipanSupplier?.Count == sta.JML_RECORD)
                                    {
                                        Log("[PARTISIPAN] Insert PARTISIPAN SUPPLIER ...");
                                        if (await TrDA.InsertPartisipanSupplier(Jobs, objPartisipanSupplier, tglStatus.ToString("yyyy-MM-dd"), tglProses.ToString("yyyy-MM-dd HH:mm:ss")))
                                        {
                                            Log("[LOG] Insert LOG SUPPLIER ...");
                                            await TrDA.InsertLogMaster(Jobs, "SUPPLIER", tglStatus.ToString("yyyy-MM-dd"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"));

                                            List<dynamic> dtToko = await TrDA.GetDraftPartisipanMaster("SUPPLIER", Jobs, tglStatus.ToString("yyyy-MM-dd"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"), null, "");
                                            ObjUtil.Tracelog(Jobs, $"[DRAFT] GET DRAFT SUPPLIER (ROWS: {dtToko}).", Utility.TipeLog.Debug);

                                            foreach (var dr in dtToko)
                                            {
                                                TrDA.InitialTblSupplierToko(Jobs, dr["KDTK"].ToString()!);
                                                var dtSupplierToko = TrDA.GetSupplierTokoByKode(Jobs, dr["SUPCO"].ToString()!);
                                                if (dtSupplierToko.Rows.Count > 0)
                                                    TrDA.InsertSupplierToko(Jobs, dr["KDTK"].ToString()!, dtSupplierToko);
                                                Log($"SUPPLIER Toko [{dr["KDTK"].ToString()!.ToUpper()}]");
                                            }
                                        }
                                    }
                                    else
                                    {
                                        ObjUtil.Tracelog(Jobs, $"[SUPPLIER] Jumlah baris PARTISIPAN tidak sesuai.\nSTATUS: {sta.JML_RECORD}\nPARTISIPAN: {objPartisipanSupplier?.Count}", Utility.TipeLog.Warning);
                                        ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[SUPPLIER] Jumlah baris PARTISIPAN tidak sesuai.\nSTATUS: {sta.JML_RECORD}\nPARTISIPAN: {objPartisipanSupplier?.Count}");
                                    }
                                }
                            }
                        }
                        else ObjUtil.Tracelog(Jobs, $"Data SUPPLIER dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.", Utility.TipeLog.Error);
                    }
                }
                catch (Exception ex) { ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error); }

                // ==================== PAJAK ====================
                try
                {
                    tglStatus = default; tglProses = default;
                    Log("[STATUS] Get PAJAK ...");
                    result = await Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "PAJAK", true);
                    var objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);

                    if (objStatus != null)
                    {
                        try { tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }
                        try { tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }
                        Log($"[STATUS] Result PAJAK : {objStatus[0].FLAG_PROSES}-{objStatus[0].JML_RECORD}");

                        if (!await TrDA.CekStatusLogMaster(Jobs, "PAJAK", tglProses))
                        {
                            if (objStatus[0].FLAG_PROSES == "Y")
                            {
                                Log("[MASTER] Get PAJAK ...");
                                result = await Service.GetMasterPajakService(Jobs, Conf);
                                var objPajak = JsonConvert.DeserializeObject<List<PAJAK>>(result);
                                Log($"[MASTER] Result PAJAK : {objPajak?.Count}-{objStatus[0].JML_RECORD}");

                                if (objPajak?.Count == objStatus[0].JML_RECORD)
                                {
                                    Log("[PARTISIPAN] Insert PARTISIPAN PAJAK ...");
                                    if (await TrDA.InsertPartisipanPajak(Jobs, objPajak, tglStatus.ToString("yyyy-MM-dd"), tglProses.ToString("yyyy-MM-dd HH:mm:ss")))
                                    {
                                        Log("[LOG] Insert LOG PAJAK ...");
                                        await TrDA.InsertLogMaster(Jobs, "PAJAK", tglStatus.ToString("yyyy-MM-dd"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"));
                                    }
                                }
                                else
                                {
                                    ObjUtil.Tracelog(Jobs, $"[PAJAK] Jumlah baris tidak sesuai.\nSTATUS: {objStatus[0].JML_RECORD}\nMASTER: {objPajak?.Count}", Utility.TipeLog.Warning);
                                    ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[PAJAK] Jumlah baris tidak sesuai.\nSTATUS: {objStatus[0].JML_RECORD}\nMASTER: {objPajak?.Count}");
                                }
                            }
                        }
                        else ObjUtil.Tracelog(Jobs, $"Data PAJAK dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.", Utility.TipeLog.Error);
                    }
                }
                catch (Exception ex) { ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error); }

                // ==================== PRODUK KHUSUS ====================
                try
                {
                    tglStatus = default; tglProses = default;
                    Log("[STATUS] Get PRODUK KHUSUS ...");
                    result = await Service.GetStatusService(Jobs, Conf, "", "PRODUK", true);
                    var objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);

                    if (objStatus != null)
                    {
                        try { tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }
                        try { tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }
                        Log($"[STATUS] Result PRODUK KHUSUS : {objStatus[0].FLAG_PROSES}-{objStatus[0].JML_RECORD}");

                        if (!await TrDA.CekStatusLogMaster_V2(Jobs, "PRODUK_KHUSUS", tglStatus))
                        {
                            if (objStatus[0].FLAG_PROSES == "Y")
                            {
                                Log("[MASTER] Get PRODUK KHUSUS ...");
                                result = await Service.GetMasterProdukKhususService(Jobs, Conf);
                                var objProdKhusus = JsonConvert.DeserializeObject<List<PRODUK_KHUSUS>>(result);
                                Log($"[MASTER] Result PRODUK KHUSUS : {objProdKhusus?.Count}-{objStatus[0].JML_RECORD}");

                                if (objProdKhusus?.Count == objStatus[0].JML_RECORD)
                                {
                                    Log("[MASTER] Insert PRODUK KHUSUS ...");
                                    if (await TrDA.InsertMasterProdukKhusus(Jobs, objProdKhusus, tglStatus, tglProses))
                                        await TrDA.InsertLogMasterV2(Jobs, "PRODUK_KHUSUS", tglStatus.ToString("yyyy-MM-dd HH:mm:ss"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"), "ALL", "", "OK", "");
                                    else ObjUtil.TraceLogFile($"ERROR|InsertMasterProduk_Khusus:\nJSON: {result}");
                                }
                                else
                                {
                                    ObjUtil.Tracelog(Jobs, $"[PRODUK_KHUSUS] Jumlah baris tidak sesuai.\nSTATUS: {objStatus[0].JML_RECORD}\nMASTER: {objProdKhusus?.Count}", Utility.TipeLog.Warning);
                                    ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[PRODUK_KHUSUS] Jumlah baris tidak sesuai.\nSTATUS: {objStatus[0].JML_RECORD}\nMASTER: {objProdKhusus?.Count}");
                                }
                            }
                        }
                        else ObjUtil.Tracelog(Jobs, $"Data PRODUK_KHUSUS dengan TGL_STATUS: {tglStatus:yyyy-MM-dd} sudah diambil.", Utility.TipeLog.Error);
                    }
                }
                catch (Exception ex) { ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error); }

                // ==================== SARANA PRODUK KHUSUS ====================
                try
                {
                    tglStatus = default; tglProses = default;
                    Log("[STATUS] Get SARANA PRODUK KHUSUS ...");
                    result = await Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "SARANA_PRODUK_KHUSUS", false);
                    var objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);

                    if (objStatus != null)
                    {
                        try { tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }
                        try { tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }

                        if (!await TrDA.CekStatusLogMaster(Jobs, "SARANA_PRODUK_KHUSUS", tglProses))
                        {
                            if (objStatus[0].FLAG_PROSES == "Y")
                            {
                                Log("[MASTER] Get SARANA PRODUK KHUSUS ...");
                                result = await Service.GetMasterSaranaProdukKhususService(Jobs, Conf.KodeCabang, Conf, false);
                                var objSaranaProdKhusus = JsonConvert.DeserializeObject<List<SARANA_PRODUK_KHUSUS>>(result);

                                if (objSaranaProdKhusus?.Count == objStatus[0].JML_RECORD)
                                {
                                    Log("[MASTER] Insert SARANA PRODUK KHUSUS ...");
                                    if (await TrDA.InsertMasterSaranaProdukKhusus(Jobs, objSaranaProdKhusus, tglStatus, tglProses))
                                    {
                                        Log("[LOG] Insert LOG SARANA PRODUK KHUSUS ...");
                                        await TrDA.InsertLogMaster(Jobs, "SARANA_PRODUK_KHUSUS", tglStatus.ToString("yyyy-MM-dd"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"));
                                    }
                                    else ObjUtil.TraceLogFile($"ERROR|InsertMasterSarana_Produk_Khusus:\nJSON: {result}");
                                }
                                else
                                {
                                    ObjUtil.Tracelog(Jobs, $"[SARANA_PRODUK_KHUSUS] Jumlah baris tidak sesuai.\nSTATUS: {objStatus[0].JML_RECORD}\nMASTER: {objSaranaProdKhusus?.Count}", Utility.TipeLog.Warning);
                                    ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[SARANA_PRODUK_KHUSUS] Jumlah baris tidak sesuai.");
                                }
                            }
                        }
                        else ObjUtil.Tracelog(Jobs, $"Data SARANA_PRODUK_KHUSUS dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.", Utility.TipeLog.Error);
                    }
                }
                catch (Exception ex) { ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error); }

                // ==================== TTT ====================
                try
                {
                    tglStatus = default; tglProses = default;
                    Log("[STATUS] Get TTT ...");
                    result = await Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "TTT", false);
                    var objStatus_V2 = JsonConvert.DeserializeObject<STATUS>(result);

                    if (objStatus_V2 != null)
                    {
                        try { tglStatus = DateTime.ParseExact(objStatus_V2.TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus_V2.TGL_STATUS); }
                        try { tglProses = DateTime.ParseExact(objStatus_V2.TGL_PROSES, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus_V2.TGL_PROSES); }
                        Log($"[STATUS] Result TTT : {objStatus_V2.FLAG_PROSES}-{objStatus_V2.JML_RECORD}");

                        if (!await TrDA.CekStatusLogMaster(Jobs, "TTT", tglProses))
                        {
                            if (objStatus_V2.FLAG_PROSES == "Y")
                            {
                                Log("[MASTER] Get TTT ...");
                                result = await Service.GetMasterTTTService(Jobs, Conf, false);
                                var objTTT = JsonConvert.DeserializeObject<List<TTT>>(result);

                                if (objTTT?.Count == objStatus_V2.JML_RECORD)
                                {
                                    Log("[MASTER] Insert TTT ...");
                                    if (await TrDA.InsertMasterTTT(Jobs, objTTT, tglStatus, tglProses))
                                    {
                                        Log("[LOG] Insert LOG TTT ...");
                                        await TrDA.InsertLogMasterV2(Jobs, "TTT", tglStatus.ToString("yyyy-MM-dd HH:mm:ss"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"), "ALL", "", "OK", "");
                                    }
                                    else ObjUtil.TraceLogFile($"ERROR|InsertMasterTTT:\nJSON: {result}");
                                }
                                else
                                {
                                    ObjUtil.Tracelog(Jobs, $"[TTT] Jumlah baris tidak sesuai.\nSTATUS: {objStatus_V2.JML_RECORD}\nMASTER: {objTTT?.Count}", Utility.TipeLog.Warning);
                                    ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[TTT] Jumlah baris tidak sesuai.");
                                }
                            }
                        }
                        else ObjUtil.Tracelog(Jobs, $"Data TTT dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.", Utility.TipeLog.Error);
                    }
                }
                catch (Exception ex) { ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error); }

                // ==================== TAT ====================
                try
                {
                    tglStatus = default; tglProses = default;
                    Log("[STATUS] Get TAT ...");
                    result = await Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "TAT", false);
                    var objStatus_V2 = JsonConvert.DeserializeObject<STATUS>(result);

                    if (objStatus_V2 != null)
                    {
                        try { tglStatus = DateTime.ParseExact(objStatus_V2.TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus_V2.TGL_STATUS); }
                        try { tglProses = DateTime.ParseExact(objStatus_V2.TGL_PROSES, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus_V2.TGL_PROSES); }
                        Log($"[STATUS] Result TAT : {objStatus_V2.FLAG_PROSES}-{objStatus_V2.JML_RECORD}");

                        if (!await TrDA.CekStatusLogMaster(Jobs, "TAT", tglProses))
                        {
                            if (objStatus_V2.FLAG_PROSES == "Y")
                            {
                                Log("[MASTER] Get TAT ...");
                                result = await Service.GetMasterTATService(Jobs, Conf, false);
                                var objTAT = JsonConvert.DeserializeObject<List<TAT>>(result);

                                if (objTAT?.Count == objStatus_V2.JML_RECORD)
                                {
                                    Log("[MASTER] Insert TAT ...");
                                    if (await TrDA.InsertMasterTAT(Jobs, objTAT, tglStatus, tglProses))
                                    {
                                        Log("[LOG] Insert LOG TAT ...");
                                        TrDA.InsertLogMasterV2(Jobs, "TAT", tglStatus.ToString("yyyy-MM-dd HH:mm:ss"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"), "ALL", "", "OK", "");
                                    }
                                    else ObjUtil.TraceLogFile($"ERROR|InsertMasterTAT:\nJSON: {result}");
                                }
                                else
                                {
                                    ObjUtil.Tracelog(Jobs, $"[TAT] Jumlah baris tidak sesuai.\nSTATUS: {objStatus_V2.JML_RECORD}\nMASTER: {objTAT?.Count}", Utility.TipeLog.Warning);
                                    ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[TAT] Jumlah baris tidak sesuai.");
                                }
                            }
                        }
                        else ObjUtil.Tracelog(Jobs, $"Data TAT dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.", Utility.TipeLog.Error);
                    }
                }
                catch (Exception ex) { ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error); }

                // ==================== TOKO_PRODSUS ====================
                try
                {
                    tglStatus = default; tglProses = default;
                    Log("[STATUS] Get TOKO_PRODSUS ...");
                    result = await Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "TOKO_PRODSUS", false);
                    var objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);

                    if (objStatus != null)
                    {
                        try { tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }
                        try { tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }

                        if (!await TrDA.CekStatusLogMaster(Jobs, "TOKO_PRODSUS", tglProses))
                        {
                            if (objStatus[0].FLAG_PROSES == "Y")
                            {
                                Log("[MASTER] Get TOKO_PRODSUS ...");
                                result = await Service.GetMasterTokoProdSusService(Jobs, Conf, false);
                                var objTokoProdSus = JsonConvert.DeserializeObject<List<TOKO_PRODSUS>>(result);

                                if (objTokoProdSus?.Count == objStatus[0].JML_RECORD)
                                {
                                    if (await TrDA.InsertMasterTokoProdSus(Jobs, objTokoProdSus, tglStatus, tglProses))
                                        await TrDA.InsertLogMasterV2(Jobs, "TOKO_PRODSUS", tglStatus.ToString("yyyy-MM-dd HH:mm:ss"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"), "ALL", "", "OK", "WRC");
                                    else ObjUtil.TraceLogFile($"ERROR|InsertMasterTokoProdSus:\nJSON: {result}");
                                }
                                else
                                {
                                    ObjUtil.Tracelog(Jobs, $"[TOKO_PRODSUS] Jumlah baris tidak sesuai.\nSTATUS: {objStatus[0].JML_RECORD}\nMASTER: {objTokoProdSus?.Count}", Utility.TipeLog.Warning);
                                    ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[TOKO_PRODSUS] Jumlah baris tidak sesuai.");
                                }
                            }
                        }
                        else ObjUtil.Tracelog(Jobs, $"Data TOKO_PRODSUS dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.", Utility.TipeLog.Error);
                    }
                }
                catch (Exception ex) { ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error); }

                // ==================== PRODSUS_JUAL_TOKO ====================
                try
                {
                    tglStatus = default; tglProses = default;
                    Log("[STATUS] Get PRODSUS_JUAL_TOKO ...");
                    result = await Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "PRODSUS_JUAL_TOKO", false);
                    var objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);

                    if (objStatus != null)
                    {
                        try { tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }
                        try { tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }

                        if (!await TrDA.CekStatusLogMaster(Jobs, "PRODSUS_JUAL_TOKO", tglProses))
                        {
                            if (objStatus[0].FLAG_PROSES == "Y")
                            {
                                Log("[MASTER] Get PRODSUS_JUAL_TOKO ...");
                                result = await Service.GetMasterProdSusJualTokoService(Jobs, Conf, false);
                                var objProdSusJualToko = JsonConvert.DeserializeObject<List<PRODSUS_JUAL_TOKO>>(result);

                                if (objProdSusJualToko?.Count == objStatus[0].JML_RECORD)
                                {
                                    if (await TrDA.InsertMasterProdSusJualToko(Jobs, objProdSusJualToko, tglStatus, tglProses))
                                        await TrDA.InsertLogMasterV2(Jobs, "PRODSUS_JUAL_TOKO", tglStatus.ToString("yyyy-MM-dd HH:mm:ss"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"), "ALL", "", "OK", "WRC");
                                    else ObjUtil.TraceLogFile($"ERROR|InsertMasterProdSusJualToko:\nJSON: {result}");
                                }
                                else
                                {
                                    ObjUtil.Tracelog(Jobs, $"[PRODSUS_JUAL_TOKO] Jumlah baris tidak sesuai.", Utility.TipeLog.Warning);
                                    ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[PRODSUS_JUAL_TOKO] Jumlah baris tidak sesuai.");
                                }
                            }
                        }
                        else ObjUtil.Tracelog(Jobs, $"Data PRODSUS_JUAL_TOKO dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.", Utility.TipeLog.Error);
                    }
                }
                catch (Exception ex) { ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error); }

                // ==================== TOKO_PRODSUS_PERIODE ====================
                try
                {
                    tglStatus = default; tglProses = default;
                    Log("[STATUS] Get TOKO_PRODSUS_PERIODE ...");
                    result = await Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "TOKO_PRODSUS_PERIODE", false);
                    var objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);

                    if (objStatus != null)
                    {
                        try { tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }
                        try { tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd", System.Globalization.DateTimeFormatInfo.InvariantInfo); } catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }

                        if (!await TrDA.CekStatusLogMaster(Jobs, "TOKO_PRODSUS_PERIODE", tglProses))
                        {
                            if (objStatus[0].FLAG_PROSES == "Y")
                            {
                                Log("[MASTER] Get TOKO_PRODSUS_PERIODE ...");
                                result = await Service.GetMasterTokoProdSusPeriodeService(Jobs, Conf, false);
                                var objTokoProdSusPeriode = JsonConvert.DeserializeObject<List<TOKO_PRODSUS_PERIODE>>(result);

                                if (objTokoProdSusPeriode?.Count == objStatus[0].JML_RECORD)
                                {
                                    if (await TrDA.InsertMasterTokoProdSusPeriode(Jobs, objTokoProdSusPeriode, tglStatus, tglProses))
                                        await TrDA.InsertLogMasterV2(Jobs, "TOKO_PRODSUS_PERIODE", tglStatus.ToString("yyyy-MM-dd HH:mm:ss"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"), "ALL", "", "OK", "WRC");
                                    else ObjUtil.TraceLogFile($"ERROR|InsertMasterTokoProdSusPeriode:\nJSON: {result}");
                                }
                                else
                                {
                                    ObjUtil.Tracelog(Jobs, $"[TOKO_PRODSUS_PERIODE] Jumlah baris tidak sesuai.", Utility.TipeLog.Warning);
                                    ObjUtil.SendEmail(_emailTo, _emailCC, $"[WARNING] TrPrCabang - {Conf.KodeCabang}", $"[TOKO_PRODSUS_PERIODE] Jumlah baris tidak sesuai.");
                                }
                            }
                        }
                        else ObjUtil.Tracelog(Jobs, $"Data TOKO_PRODSUS_PERIODE dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.", Utility.TipeLog.Error);
                    }
                }
                catch (Exception ex) { ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error); }
            }
            catch (Exception ex)
            {
                ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error);
                ObjUtil.TraceLogFile($"ERROR: {ex.Message}\n{ex.StackTrace}\nJSON: {result}");
                ObjUtil.SendEmail(_emailTo, _emailCC, $"[ERROR] TrPrCabang - {Conf.KodeCabang}",
                    $"Proses GET TrPr Cabang ({Conf.KodeCabang}) terhenti:\n{ex.Message}\n{ex.StackTrace}");
            }
        }

        private static async Task GetComplete()
        {
            await TrDA.UpdateConst(Jobs, "TRPRGET", AppDomain.CurrentDomain.FriendlyName, DateTime.Now, DateTime.Now);
            CloseConnection(ConnTrPr);
            Log("Finish ...");
            ObjUtil.Tracelog(Jobs, $"{AppDomain.CurrentDomain.FriendlyName} DOWN.", Utility.TipeLog.Debug);
            Thread.Sleep(3000);
        }

        // =====================================================================
        // GET2
        // =====================================================================
        private async Task GetMaster2DoWork()
        {
            string result = "";
            DateTime tglStatus = default;
            DateTime tglProses = default;

            // Helper local untuk pattern berulang
            async Task ProcessMaster<T>(string masterName, string jenis, bool useKodeCabang, Func<Task<string>> fetchStatus, Func<Task<string>> fetchMaster, Func<List<T>?, DateTime, DateTime, Task<bool>> insertAction) where T : class
            {
                try
                {
                    DateTime tglStatus = default;
                    DateTime tglProses = default;

                    Log($"[STATUS] Get {masterName} ...");

                    var result = await fetchStatus(); // ✅ await
                    var objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);

                    if (objStatus == null || objStatus.Count == 0) return;

                    try
                    {
                        tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd",
                            System.Globalization.CultureInfo.InvariantCulture);
                    }
                    catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }

                    try
                    {
                        tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd",
                            System.Globalization.CultureInfo.InvariantCulture);
                    }
                    catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }

                    Log($"[STATUS] Result {masterName} : {objStatus[0].FLAG_PROSES}-{objStatus[0].JML_RECORD}");

                    if (await TrDA.CekStatusLogMaster(Jobs, jenis, tglProses))
                    {
                        ObjUtil.Tracelog(Jobs,
                            $"Data Master {masterName} dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.",
                            Utility.TipeLog.Error);
                        return;
                    }

                    if (objStatus[0].FLAG_PROSES != "Y") return;

                    Log($"[MASTER] Get {masterName} ...");

                    result = await fetchMaster(); // ✅ await
                    var objMaster = JsonConvert.DeserializeObject<List<T>>(result);

                    Log($"[MASTER] Result {masterName} : {objMaster?.Count}-{objStatus[0].JML_RECORD}");

                    if (objMaster?.Count == objStatus[0].JML_RECORD)
                    {
                        Log($"[MASTER] Insert {masterName} ...");

                        bool success = await insertAction(objMaster, tglStatus, tglProses); // ✅ await

                        if (!success)
                            ObjUtil.TraceLogFile($"ERROR|Insert{masterName}:\nJSON: {result}");
                    }
                    else
                    {
                        ObjUtil.Tracelog(Jobs,
                            $"[{jenis}] Jumlah baris tidak sesuai.\nSTATUS: {objStatus[0].JML_RECORD}\nMASTER: {objMaster?.Count}",
                            Utility.TipeLog.Warning);

                        ObjUtil.SendEmail(_emailTo, _emailCC,
                            $"[WARNING] TrPrCabang - {Conf.KodeCabang}",
                            $"[{jenis}] Jumlah baris tidak sesuai.\nSTATUS: {objStatus[0].JML_RECORD}\nMASTER: {objMaster?.Count}");
                    }
                }
                catch (Exception ex)
                {
                    ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error);
                }
            }


            await ProcessMaster<TKX>("TKX", "TKX", true, async () => await Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "TKX", false), async () => await Service.GetMasterTKXService(Jobs, Conf, false),
                async (data, ts, tp) =>
                {
                    if (await MasDA.InsertMasterTKX(Jobs, data!, ts, tp))
                    {
                        await TrDA.InsertLogMaster(
                            Jobs,
                            "TKX",
                            ts.ToString("yyyy-MM-dd"),
                            tp.ToString("yyyy-MM-dd HH:mm:ss")
                        );
                        return true;
                    }
                    return false;
                }
            );

            // TKX
            await ProcessMaster<TKX>(
                "TKX", "TKX", true,
                () => Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "TKX", false),
                () => Service.GetMasterTKXService(Jobs, Conf, false),
                async (data, ts, tp) =>
                {
                    if (await MasDA.InsertMasterTKX(Jobs, data!, ts, tp))
                    {
                        await TrDA.InsertLogMaster(
                            Jobs,
                            "TKX",
                            ts.ToString("yyyy-MM-dd"),
                            tp.ToString("yyyy-MM-dd HH:mm:ss")
                        );
                        return true;
                    }
                    return false;
                }
            );

            // DCX
            await ProcessMaster<DCX>(
                "DCX", "DCX", true,
                () => Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "DCX", false),
                () => Service.GetMasterDCXService(Jobs, Conf, false),
                async (data, ts, tp) =>
                {
                    if (await MasDA.InsertMasterDCX(Jobs, data!, ts, tp))
                    {
                        await TrDA.InsertLogMaster(
                            Jobs,
                            "DCX",
                            ts.ToString("yyyy-MM-dd"),
                            tp.ToString("yyyy-MM-dd HH:mm:ss")
                        );
                        return true;
                    }
                    return false;
                }
            );

            // DIVISI
            await ProcessMaster<DIVISI>(
                "DIVISI", "DIVISI", true,
                () => Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "DIVISI", false),
                () => Service.GetMasterDIV_DEPT_KAT_Service(Jobs, Conf, "DIVISI", false),
                async (data, ts, tp) =>
                {
                    if (await MasDA.InsertMasterDIVISI(Jobs, data!, ts, tp))
                    {
                        await TrDA.InsertLogMasterByListToko(
                            Jobs, "DIVISI",
                            ts.ToString("yyyy-MM-dd HH:mm:ss"),
                            tp.ToString("yyyy-MM-dd HH:mm:ss"),
                            "OK", ""
                        );
                        return true;
                    }
                    return false;
                }
            );

            // DEPT
            await ProcessMaster<DEPT>(
                "DEPT", "DEPT", true,
                () => Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "DEPT", false),
                () => Service.GetMasterDIV_DEPT_KAT_Service(Jobs, Conf, "DEPT", false),
                async (data, ts, tp) =>
                {
                    if (await MasDA.InsertMasterDEPT(Jobs, data!, ts, tp))
                    {
                        await TrDA.InsertLogMasterByListToko(
                            Jobs, "DEPT",
                            ts.ToString("yyyy-MM-dd HH:mm:ss"),
                            tp.ToString("yyyy-MM-dd HH:mm:ss"),
                            "OK", ""
                        );
                        return true;
                    }
                    return false;
                }
            );

            // KATEGORI
            await ProcessMaster<KATEGORI>(
                "KATEGORI", "KATEGORI", true,
                () => Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "KATEGORI", false),
                () => Service.GetMasterDIV_DEPT_KAT_Service(Jobs, Conf, "KATEGORI", false),
                async (data, ts, tp) =>
                {
                    if (await MasDA.InsertMasterKATEGORI(Jobs, data!, ts, tp))
                    {
                        await TrDA.InsertLogMasterByListToko(
                            Jobs, "KATEGORI",
                            ts.ToString("yyyy-MM-dd HH:mm:ss"),
                            tp.ToString("yyyy-MM-dd HH:mm:ss"),
                            "OK", ""
                        );
                        return true;
                    }
                    return false;
                }
            );

            // TAT_PRD
            await ProcessMaster<TAT_PRD>(
                "TAT_PRD", "TAT_PRD", true,
                () => Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "TAT_PRD", true),
                () => Service.GetMasterTAT_PRD_Service(Jobs, Conf, true),
                async (data, ts, tp) =>
                {
                    if (await MasDA.InsertMasterTATPrd(Jobs, data!, ts, tp))
                    {
                        await TrDA.InsertLogMasterByListTokoExtended(
                            Jobs, "TAT_PRD",
                            ts.ToString("yyyy-MM-dd HH:mm:ss"),
                            tp.ToString("yyyy-MM-dd HH:mm:ss"),
                            "OK", "", Conf.KodeCabang
                        );
                        return true;
                    }
                    return false;
                }
            );


            async Task ProcessMasterKRAT<T>(string masterName, string jenis, Func<Task<string>> fetchStatus, Func<Task<string>> fetchMaster, Func<List<T>, STATUS, DateTime, DateTime, Task> processAction) where T : class
            {
                try
                {
                    DateTime tglStatus = default;
                    DateTime tglProses = default;

                    Log($"[STATUS] Get {masterName} ...");

                    var result = await fetchStatus();
                    var objStatus = JsonConvert.DeserializeObject<List<STATUS>>(result);

                    if (objStatus == null || objStatus.Count == 0) return;

                    try
                    {
                        tglStatus = DateTime.ParseExact(objStatus[0].TGL_STATUS, "yyyy-MM-dd",
                            System.Globalization.CultureInfo.InvariantCulture);
                    }
                    catch { tglStatus = DateTime.Parse(objStatus[0].TGL_STATUS); }

                    try
                    {
                        tglProses = DateTime.ParseExact(objStatus[0].TGL_PROSES, "yyyy-MM-dd",
                            System.Globalization.CultureInfo.InvariantCulture);
                    }
                    catch { tglProses = DateTime.Parse(objStatus[0].TGL_PROSES); }

                    Log($"[STATUS] Result {masterName} : {objStatus[0].FLAG_PROSES}-{objStatus[0].JML_RECORD}");

                    if (await TrDA.CekStatusLogMaster(Jobs, jenis, tglProses))
                    {
                        ObjUtil.Tracelog(Jobs,
                            $"Data Master {masterName} dengan TGL_PROSES: {tglProses:yyyy-MM-dd} sudah diambil.",
                            Utility.TipeLog.Error);
                        return;
                    }

                    if (objStatus[0].FLAG_PROSES != "Y") return;

                    Log($"[MASTER] Get {masterName} ...");

                    result = await fetchMaster();
                    var objMaster = JsonConvert.DeserializeObject<List<T>>(result);

                    Log($"[MASTER] Result {masterName} : {objMaster?.Count}-{objStatus[0].JML_RECORD}");

                    if (objMaster == null || objMaster.Count == 0) return;

                    if (objMaster.Count == objStatus[0].JML_RECORD)
                    {
                        await processAction(objMaster, objStatus[0], tglStatus, tglProses);
                    }
                    else
                    {
                        ObjUtil.Tracelog(Jobs,
                            $"[{jenis}] Jumlah baris tidak sesuai.\nSTATUS: {objStatus[0].JML_RECORD}\nMASTER: {objMaster?.Count}",
                            Utility.TipeLog.Warning);

                        ObjUtil.SendEmail(_emailTo, _emailCC,
                            $"[WARNING] TrPrCabang - {Conf.KodeCabang}",
                            $"[{jenis}] Jumlah baris tidak sesuai.");
                    }
                }
                catch (Exception ex)
                {
                    ObjUtil.Tracelog(Jobs, $"{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error);
                }
            }


            await ProcessMasterKRAT<KRAT>("PLU_KRAT", "PLU_KRAT",
                () => Service.GetStatusService(Jobs, Conf, Conf.KodeCabang, "PLU_KRAT", true),
                () => Service.GetMasterPLU_KRAT_Service(Jobs, Conf, true), async (objKRAT, status, tglStatus, tglProses) =>
                {
                    Log("[MASTER] Send Master PLU_KRAT ...");

                    var objComp = new CompressHelper(ObjUtil);
                    var httpClient = new HttpClient();
                    var objSvr = new ServiceController(httpClient, ObjUtil, objComp);

                    string msgProcess = "";

                    var sb = new StringBuilder();
                    sb.Append("CREATE TABLE IF NOT EXISTS `posservice_base`.`mst_plu_krat` (");
                    sb.Append(" `PLU` VARCHAR(10) NOT NULL,");
                    sb.Append(" `UPDID` VARCHAR(50) DEFAULT NULL,");
                    sb.Append(" `UPDTIME` DATETIME DEFAULT NULL,");
                    sb.Append(" PRIMARY KEY (`PLU`)) ENGINE=INNODB DEFAULT CHARSET=latin1; ");

                    sb.Append("TRUNCATE `posservice_base`.`mst_plu_krat`; ");
                    sb.Append("INSERT INTO `posservice_base`.`mst_plu_krat` (`PLU`,`UPDID`,`UPDTIME`) VALUES ");

                    foreach (var itm in objKRAT)
                        sb.Append($"('{itm.PLU}',USER(),NOW()),");

                    string sqlCreate = sb.ToString().TrimEnd(',') +
                        " ON DUPLICATE KEY UPDATE `UPDID`=USER(),`UPDTIME`=NOW();";

                    byte[] sqlByte = objComp.Compress(sqlCreate);

                    bool sendOk = Conf.IsPosRT_RestApi
                        ? await objSvr.ExecuteQueryPosrt_RestApi(Jobs, sqlCreate, Conf, msg => msgProcess = msg)
                        : await objSvr.ExecuteQueryPosrt_Service(Jobs, sqlByte, Conf, msg => msgProcess = msg);

                    if (sendOk)
                    {
                        await TrDA.InsertLogMasterV2(
                            Jobs,
                            "PLU_KRAT",
                            tglStatus.ToString("yyyy-MM-dd HH:mm:ss"),
                            tglProses.ToString("yyyy-MM-dd HH:mm:ss"),
                            "PSRT", "", "OK", "OK"
                        );
                    }
                    else
                    {
                        string urlInfo = Conf.IsPosRT_RestApi ? Conf.UrlPosRT_RestApi : Conf.UrlPosRT_Service;

                        ObjUtil.Tracelog(Jobs,
                            $"ERROR|ExecuteQueryPosrt : PLU_KRAT\nURL : {urlInfo}\nMessage : {msgProcess}",
                            Utility.TipeLog.Debug);

                        ObjUtil.TraceLogFile(
                            $"ERROR|ExecuteQueryPosrt : PLU_KRAT\nURL : {urlInfo}\nMessage : {msgProcess}"
                        );
                    }
                }
            );

        }

        private static void GetMaster2Complete()
        {
            TrDA.UpdateConst(Jobs, "TRPRGET2", AppDomain.CurrentDomain.FriendlyName, DateTime.Now, DateTime.Now);
            CloseConnection(ConnTrPr);
            Log("Finish ...");
            ObjUtil.Tracelog(Jobs, $"{AppDomain.CurrentDomain.FriendlyName} DOWN.", Utility.TipeLog.Debug);
            Thread.Sleep(3000);
        }

        // =====================================================================
        // SEND
        // =====================================================================
        private static async Task SendDoWork(int idThread, string listToko)
        {
            try
            {
                ObjUtil.Tracelog(Jobs, $"THREAD {idThread} SendDoWork START", Utility.TipeLog.Debug);

                // ==================== PROMOSI ====================
                var dtPromoGroupID = await TrDA.GetListPromoGroupID(Jobs);
                Log($"THREAD {idThread}|[PARTISIPAN] Draft PARTISIPAN PROMOSI ...");
                List<dynamic> draftPartisipan = await TrDA.GetDraftPartisipanMaster("PROMO", Jobs, "", "", dtPromoGroupID, listToko);
                ObjUtil.Tracelog(Jobs, $"THREAD {idThread}|Draft PARTISIPAN PROMOSI FINISH\nRows: {draftPartisipan.Count}", Utility.TipeLog.Debug);

                if (draftPartisipan.Count > 0)
                {
                    Log($"THREAD {idThread}|[PROMO] Send Promo Toko ...");
                    foreach (var dr in draftPartisipan)
                    {
                        var dtPromoToko = TrDA.GetPromoTokoByKode(Jobs, dr["KD_TOKO"].ToString()!, dr["KD_PROMO"].ToString()!, dtPromoGroupID);
                        if (dtPromoToko.Rows.Count > 0)
                        {
                            DateTime tglStatus = (DateTime)dr["TGL_STATUS"];
                            DateTime tglProses = (DateTime)dr["TGL_PROSES"];
                            if (PoDA.InsertTrPrPromoTask(Jobs, dtPromoToko, dr["KD_TOKO"].ToString()!, tglStatus, Conf, idThread))
                                TrDA.UpdateLogMaster(Jobs, "PROMO", tglStatus.ToString("yyyy-MM-dd"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"), dr["KD_TOKO"].ToString()!, dr["IDTOKO"].ToString()!, "", $"OK|T:{idThread}");
                        }
                        Log($"THREAD {idThread}|Promo Task [{dr["KD_TOKO"].ToString()!.ToUpper()}]");
                        Thread.Sleep(100);
                    }
                }

                // ==================== PAJAK ====================
                // draftPartisipan = new DataTable();
                Log($"THREAD {idThread}|[PARTISIPAN] Draft PARTISIPAN PAJAK ...");
                draftPartisipan = await TrDA.GetDraftPartisipanMaster("PAJAK", Jobs, "", "", null, listToko);

                if (draftPartisipan.Count > 0)
                {
                    Log($"THREAD {idThread}|[PAJAK] Send Pajak Toko ...");
                    foreach (var dr in draftPartisipan)
                    {
                        DateTime tglStatus = (DateTime)dr["TGL_STATUS"];
                        DateTime tglProses = (DateTime)dr["TGL_PROSES"];
                        bool ok = PoDA.InsertTrPrPajakTask(Jobs, dr, Conf, idThread);
                        string statusCode = ok ? $"OK|T:{idThread}" : $"NOK|T:{idThread}";
                        TrDA.UpdateLogMaster(Jobs, "PAJAK", tglStatus.ToString("yyyy-MM-dd"), tglProses.ToString("yyyy-MM-dd HH:mm:ss"), dr["TOK_CODE"].ToString()!, dr["IDTOKO"].ToString()!, "", statusCode);
                        Log($"THREAD {idThread}|Pajak Task [{dr["TOK_CODE"].ToString()!.ToUpper()}]");
                        Thread.Sleep(100);
                    }
                }

                // ==================== PRODUK_KHUSUS ====================
                // draftPartisipan = new DataTable();
                if (Conf.SendMastProdSus && idThread == 0)
                {
                    Log($"THREAD {idThread}|[PARTISIPAN] Draft PARTISIPAN PRODUK_KHUSUS ...");
                    draftPartisipan = await TrDA.GetDraftPartisipanMaster("PRODUK_KHUSUS", Jobs, "", "", null, listToko);

                    if (draftPartisipan.Count > 0)
                    {
                        Log($"THREAD {idThread}|[PRODUK_KHUSUS] Send PRODUK_KHUSUS Toko ...");
                        foreach (var dr in draftPartisipan)
                        {
                            string tglStatus = dr["TGL_STATUS"].ToString()!;
                            string tglProses = dr["TGL_PROSES"].ToString()!;
                            int idToko = int.TryParse(dr["IDTOKO"].ToString(), out int id) ? id : 0;
                            bool ok = PoDA.SendProdukKhusus_PersediaanToko(Jobs, dr["KODETOKO"].ToString()!, tglStatus, tglProses, Conf, idThread);
                            string statusCode = ok ? $"OK|T:{idThread}" : $"NOK|T:{idThread}";
                            TrDA.UpdateLogMaster(Jobs, "PRODUK_KHUSUS", tglStatus, tglProses, dr["KODETOKO"].ToString()!, idToko, "", statusCode);
                            Log($"THREAD {idThread}|PRODUK_KHUSUS Task [{dr["KODETOKO"].ToString()!.ToUpper()}]");
                            Thread.Sleep(100);
                        }
                    }
                }
                else ObjUtil.Tracelog(Jobs, $"Setting SEND MASTER PRODUK KHUSUS [PASSPT] = {Conf.SendMastProdSus}", Utility.TipeLog.Debug);

                // ==================== SARANA_PRODUK_KHUSUS ====================
                // draftPartisipan = new DataTable();
                Log($"THREAD {idThread}|[PARTISIPAN] Draft PARTISIPAN SARANA_PRODUK_KHUSUS ...");
                draftPartisipan = await TrDA.GetDraftPartisipanMaster("SARANA_PRODUK_KHUSUS", Jobs, "", "", null, listToko);

                if (draftPartisipan.Count > 0)
                {
                    Log($"THREAD {idThread}|[SARANA_PRODUK_KHUSUS] Send SARANA_PRODUK_KHUSUS Toko ...");
                    foreach (var dr in draftPartisipan)
                    {
                        string tglStatus = dr["TGL_STATUS"].ToString()!;
                        string tglProses = dr["TGL_PROSES"].ToString()!;
                        bool ok = PoDA.InsertSaranaProdukKhusus_Task(Jobs, dr["KODETOKO"].ToString()!, tglStatus, tglProses, Conf, idThread);
                        string statusCode = ok ? $"OK|T:{idThread}" : $"NOK|T:{idThread}";
                        TrDA.UpdateLogMaster(Jobs, "SARANA_PRODUK_KHUSUS", tglStatus, tglProses, dr["KODETOKO"].ToString()!, dr["IDTOKO"].ToString()!, "", statusCode);
                        Log($"THREAD {idThread}|SARANA_PRODUK_KHUSUS Task [{dr["KODETOKO"].ToString()!.ToUpper()}]");
                        Thread.Sleep(100);
                    }
                }

                // ==================== TAT ====================
                // draftPartisipan = new DataTable();
                if (idThread == 0)
                {
                    Log($"THREAD {idThread}|[PARTISIPAN] Draft PARTISIPAN TAT ...");
                    draftPartisipan = await TrDA.GetDraftPartisipanMaster("TAT", Jobs, "", "", null, listToko);

                    if (draftPartisipan.Count > 0)
                    {
                        foreach (var dr in draftPartisipan)
                        {
                            string tglStatus = dr["TGL_STATUS"].ToString()!;
                            string tglProses = dr["TGL_PROSES"].ToString()!;
                            int idToko = int.TryParse(dr["IDTOKO"].ToString(), out int id) ? id : 0;
                            bool ok = PoDA.SendTAT_PersediaanToko(Jobs, dr["KODETOKO"].ToString()!, tglStatus, tglProses, Conf, idThread);
                            string statusCode = ok ? $"OK|T:{idThread}" : $"NOK|T:{idThread}";
                            TrDA.UpdateLogMaster(Jobs, "TAT", tglStatus, tglProses, dr["KODETOKO"].ToString()!, idToko, "", statusCode);
                            Log($"THREAD {idThread}|TAT Task [{dr["KODETOKO"].ToString()!.ToUpper()}]");
                            Thread.Sleep(100);
                        }
                    }
                }

                // ==================== TTT ====================
                // draftPartisipan = new DataTable();
                if (idThread == 0)
                {
                    Log($"THREAD {idThread}|[PARTISIPAN] Draft PARTISIPAN TTT ...");
                    draftPartisipan = await TrDA.GetDraftPartisipanMaster("TTT", Jobs, "", "", null, listToko);

                    if (draftPartisipan.Count > 0)
                    {
                        foreach (var dr in draftPartisipan)
                        {
                            string tglStatus = dr["TGL_STATUS"].ToString()!;
                            string tglProses = dr["TGL_PROSES"].ToString()!;
                            int idToko = int.TryParse(dr["IDTOKO"].ToString(), out int id) ? id : 0;
                            bool ok = PoDA.SendTTT_PersediaanToko(Jobs, dr["KODETOKO"].ToString()!, tglStatus, tglProses, Conf, idThread);
                            string statusCode = ok ? $"OK|T:{idThread}" : $"NOK|T:{idThread}";
                            TrDA.UpdateLogMaster(Jobs, "TTT", tglStatus, tglProses, dr["KODETOKO"].ToString()!, idToko, "", statusCode);
                            Log($"THREAD {idThread}|TTT Task [{dr["KODETOKO"].ToString()!.ToUpper()}]");
                            Thread.Sleep(100);
                        }
                    }
                }

                Log($"THREAD {idThread}|Finish");
                ObjUtil.Tracelog(Jobs, $"THREAD {idThread} SendDoWork FINISH", Utility.TipeLog.Debug);
                Thread.Sleep(1000);
            }
            catch (Exception ex)
            {
                ObjUtil.Tracelog(Jobs, $"THREAD{idThread}|{ex.Message}\n{ex.StackTrace}", Utility.TipeLog.Error);
                ObjUtil.SendEmail(_emailTo, _emailCC, $"[ERROR] TrPrCabang - {Conf.KodeCabang}",
                    $"Proses SEND TrPr Cabang ({Conf.KodeCabang}) terhenti:\nTHREAD{idThread}: \n{ex.Message}\n{ex.StackTrace}");
            }
        }

        private static async Task SendComplete()
        {
            await TrDA.UpdateConst(Jobs, "TRPRSEND", AppDomain.CurrentDomain.FriendlyName, DateTime.Now, DateTime.Now);
            CloseConnection(ConnTrPr);
            Log("Finish ...");
            ObjUtil.Tracelog(Jobs, $"{AppDomain.CurrentDomain.FriendlyName} DOWN.", Utility.TipeLog.Debug);
            Thread.Sleep(3000);
        }

        // =====================================================================
        // Helpers
        // =====================================================================
        private static void Log(string message) =>
            Console.WriteLine($"{DateTime.Now:yyyy-MM-dd HH:mm:ss} | {message}");

        private static void CloseConnection(MySqlConnection conn)
        {
            if (conn != null && conn.State != ConnectionState.Closed)
                conn.Close();
        }
    }
}