using Dapper;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net.Sockets;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using TrPrCabang_NetCore.Connection;
using TrPrCabang_NetCore.Models;

namespace TrPrCabang_NetCore.DataAccess
{
    public class ConfigDA
    {
        private readonly Utility _objUtil;
        private readonly IDbServices db;

        public ConfigDA(IDbServices db)
        {
            this.db = db;
        }

        public string GetKodeCabang(string jobs, MySqlConnection connCabang)
        {
            using var dtService = db.CreateConnection();

            string rtn = "";
            string sql = string.Empty;

            try
            {
                sql = $"SELECT nilai FROM `{db.DatabaseName()}`.`config` WHERE rkey = 'KDCB';";
                var temp = dtService.ExecuteScalar(sql);
                if (temp == null || temp.ToString() == "")
                {
                    sql = $"INSERT IGNORE `{db.DatabaseName()}`.`config` (`rkey`,`nilai`,`ket`) VALUES ('KDCB','','KODE CABANG');";
                    dtService.ExecuteAsync(sql);
                }
                else
                {
                    rtn = temp.ToString();
                }
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
            }
            return rtn;
        }

        public string GetUrlService(string jobs, MySqlConnection connCabang)
        {
            using var dtService = db.CreateConnection();
            string rtn = "";
            string sql = string.Empty;
            try
            {
                sql = $"SELECT nilai FROM `{db.DatabaseName()}`.`config` WHERE rkey = 'URLS';";
                var temp = dtService.ExecuteScalar(sql);
                if (temp == null || temp.ToString() == "")
                {
                    sql = $"INSERT IGNORE `{db.DatabaseName()}`.`config` (`rkey`,`nilai`,`ket`) VALUES ('URLS','','URL WEB SERVICE SD4');";
                    dtService.ExecuteAsync(sql);
                }
                else
                {
                    rtn = temp.ToString();
                }
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
            }
            return rtn;
        }

        public int GetTimeoutService(string jobs, MySqlConnection connCabang)
        {
            using var dtService = db.CreateConnection();
            string sql = string.Empty;
            int rtn = 0;
            try
            {
                sql = $"SELECT nilai FROM `{db.DatabaseName()}`.`config` WHERE rkey = 'TMOS';";
                var temp = dtService.ExecuteScalar(sql);
                if (temp == null || temp.ToString() == "")
                {
                    sql = $"INSERT IGNORE `{db.DatabaseName()}`.`config` (`rkey`,`nilai`,`ket`) VALUES ('TMOS','','TIME OUT WEB SERVICE (MILISECOND)');";
                    dtService.ExecuteAsync(sql);
                }
                else
                {
                    rtn = Convert.ToInt32(temp);
                }
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
            }
            return rtn;
        }

        public string GetLocalIPAddress()
        {
            string hostName = Dns.GetHostName();
            IPHostEntry hostEntry = Dns.GetHostEntry(hostName);
            foreach (IPAddress ip in hostEntry.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                    return ip.ToString();
            }
            return "";
        }

        public Config GetConfig(string jobs, string info)
        {
            using var dtService = db.CreateConnection();
            string sql = string.Empty;
            var rtn = new Config();
            info = "";
            try
            {

                // Revisi (27 Oktober 2023) : Get Ip Address Client (Local)
                rtn.IpClient = GetLocalIPAddress();

                // Helper local: baca scalar, handle null/DBNull → return ""
                string ReadScalar() => dtService.ExecuteScalar(sql) is object v && v != DBNull.Value ? v.ToString()! : "";
                long ReadCount() => Convert.ToInt64(dtService.ExecuteScalar(sql));

                void InsertIgnore(string rkey, string nilai, string ket, string nilai2 = null)
                {
                    if (nilai2 != null)
                        sql = $"INSERT IGNORE INTO `{db.DatabaseName()}`.`config` (`RKEY`,`NILAI`,`NILAI2`,`KET`) VALUES ('{rkey}','{nilai}','{nilai2}','{ket}');";
                    else
                        sql = $"INSERT IGNORE INTO `{db.DatabaseName()}`.`config` (`RKEY`,`NILAI`,`KET`) VALUES ('{rkey}','{nilai}','{ket}');";

                    dtService.ExecuteAsync(sql);
                }

                string GetNilai(string rkey)
                {
                    sql = $"SELECT NILAI FROM `{db.DatabaseName()}`.`config` WHERE RKEY = '{rkey}';";
                    return ReadScalar();
                }

                string GetNilai2(string rkey)
                {
                    sql = $"SELECT NILAI2 FROM `{db.DatabaseName()}`.`config` WHERE RKEY = '{rkey}';";
                    return ReadScalar();
                }

                long CountKey(string rkey)
                {
                    sql = $"SELECT COUNT(NILAI) FROM `{db.DatabaseName()}`.`config` WHERE RKEY = '{rkey}';";
                    return ReadCount();
                }

                // --- KDCB ---
                if (CountKey("KDCB") == 0)
                {
                    info += "Setting Kode Cabang [KDCB] Masih Kosong (Table: Config).\r\n";
                    InsertIgnore("KDCB", "", "KODE CABANG");
                }
                else
                {
                    rtn.KodeCabang = GetNilai("KDCB");
                    if (rtn.KodeCabang?.Trim().Length == 0)
                        info += "Setting Kode Cabang [KDCB] Masih Kosong (Table: Config).\r\n";
                }

                // --- TMOS ---
                if (CountKey("TMOS") == 0)
                {
                    InsertIgnore("TMOS", "60000", "TIME OUT WEB SERVICE (MILISECOND)");
                    rtn.TimeoutService = 60000;
                }
                else
                {
                    rtn.TimeoutService = Convert.ToInt32(GetNilai("TMOS"));
                    if (rtn.TimeoutService == 0) rtn.TimeoutService = 60000;
                }

                // Helper untuk setting URL + Deli (NILAI & NILAI2)
                void LoadUrlDeli(string rkey, string label, Action<string> setUrl, Action<string> setDeli)
                {
                    if (CountKey(rkey) == 0)
                    {
                        info += $"Setting {label} [{rkey}] Masih Kosong (Table: Config).\r\n";
                        InsertIgnore(rkey, "", label, "|");
                    }
                    else
                    {
                        string deli = GetNilai2(rkey);
                        if (string.IsNullOrEmpty(deli)) deli = "|";
                        setDeli(deli);

                        string url = GetNilai(rkey);
                        setUrl(url);
                        if (url?.Trim().Length == 0)
                            info += $"Setting {label} [{rkey}] Masih Kosong (Table: Config).\r\n";
                    }
                }

                // Helper untuk setting URL saja (tanpa NILAI2)
                void LoadUrl(string rkey, string label, Action<string> setUrl, bool required = true)
                {
                    if (CountKey(rkey) == 0)
                    {
                        if (required) info += $"Setting {label} [{rkey}] Masih Kosong (Table: Config).\r\n";
                        InsertIgnore(rkey, "", label, "");
                    }
                    else
                    {
                        string url = GetNilai(rkey);
                        setUrl(url);
                        if (required && url?.Trim().Length == 0)
                            info += $"Setting {label} [{rkey}] Masih Kosong (Table: Config).\r\n";
                    }
                }

                LoadUrlDeli("AST", "API GET STATUS PAJAK", v => rtn.UrlStatusPajak = v, v => rtn.DeliStatusPajak = v);
                LoadUrlDeli("AMT", "API GET MASTER PAJAK", v => rtn.UrlMasterPajak = v, v => rtn.DeliMasterPajak = v);
                LoadUrlDeli("ASS", "API GET STATUS SUPPLIER", v => rtn.UrlStatusSupplier = v, v => rtn.DeliStatusSupplier = v);
                LoadUrlDeli("AMS", "API GET MASTER SUPPLIER", v => rtn.UrlMasterSupplier = v, v => rtn.DeliMasterSupplier = v);
                LoadUrlDeli("APS", "API GET PARTISIPAN SUPPLIER", v => rtn.UrlPartisipanSupplier = v, v => rtn.DeliPartisipanSupplier = v);
                LoadUrlDeli("ASP", "API GET STATUS PROMOSI", v => rtn.UrlStatusPromo = v, v => rtn.DeliStatusPromo = v);
                LoadUrlDeli("AMP", "API GET MASTER PROMOSI", v => rtn.UrlMasterPromo = v, v => rtn.DeliMasterPromo = v);
                LoadUrlDeli("APP", "API GET PARTISIPAN PROMOSI", v => rtn.UrlPartisipanPromo = v, v => rtn.DeliPartisipanPromo = v);

                // --- POSR ---
                if (CountKey("POSR") == 0)
                {
                    info += "Setting API POSREALTIME CABANG [POSR] Masih Kosong (Table: Config).\r\n";
                    InsertIgnore("POSR", "", "API POSREALTIME CABANG");
                }
                else
                {
                    rtn.UrlPosRT_Service = GetNilai("POSR");
                    if (rtn.UrlPosRT_Service?.Trim().Length == 0)
                        info += "Setting API POSREALTIME CABANG [POSR] Masih Kosong (Table: Config).\r\n";
                }

                // --- SRAPOSC ---
                if (CountKey("SRAPOSC") == 0)
                {
                    InsertIgnore("SRAPOSC", "False", "SEND REST API POSREALTIME CABANG");
                    rtn.IsPosRT_RestApi = false;
                }
                else
                {
                    string sendApi = GetNilai("SRAPOSC");
                    rtn.IsPosRT_RestApi = sendApi?.Trim().ToUpper() == "TRUE";
                }

                if (rtn.IsPosRT_RestApi)
                {
                    LoadUrl("URAPOSC", "URL REST API POSREALTIME CABANG", v => rtn.UrlPosRT_RestApi = v);
                    LoadUrl("AURAPC", "AUTH USERNAME REST API POSREALTIME CABANG", v => rtn.AuthUserPosRT_RestApi = v);
                    LoadUrl("APRAPC", "AUTH PASSWORD REST API POSREALTIME CABANG", v => rtn.AuthPassPosRT_RestApi = v);
                }

                // --- MAXT ---
                if (CountKey("MAXT") == 0)
                {
                    InsertIgnore("MAXT", "1", "THREAD PROSES KIRIM (MAX : 5)");
                    rtn.MaxThread = 1;
                }
                else
                {
                    rtn.MaxThread = Convert.ToInt32(GetNilai("MAXT"));
                    if (rtn.UrlPosRT_Service?.Trim().Length == 0) rtn.MaxThread = 1;
                }

                LoadUrl("ASPK", "API GET STATUS PRODUK KHUSUS", v => rtn.UrlStatusProduk = v);
                LoadUrl("AMPK", "API GET MASTER PRODUK KHUSUS", v => rtn.UrlMasterProduk = v);
                LoadUrl("ASSP", "API GET STATUS SARANA PRODUK KHUSUS", v => rtn.UrlStatusSaranaProduk = v);
                LoadUrl("AMSP", "API GET MASTER SARANA PRODUK KHUSUS", v => rtn.UrlMasterSaranaProduk = v);
                LoadUrl("ASTKX", "API GET STATUS TKX", v => rtn.UrlStatusTKX = v);
                LoadUrl("AMTKX", "API GET MASTER TKX", v => rtn.UrlMasterTKX = v);
                LoadUrl("ASDCX", "API GET STATUS DCX", v => rtn.UrlStatusDCX = v);
                LoadUrl("AMDCX", "API GET MASTER DCX", v => rtn.UrlMasterDCX = v);
                LoadUrl("ASDIV", "API GET STATUS DIVISI", v => rtn.UrlStatusDIVISI = v);
                LoadUrl("AMDIV", "API GET MASTER DIVISI", v => rtn.UrlMasterDIVISI = v);
                LoadUrl("ASDEP", "API GET STATUS DEPT", v => rtn.UrlStatusDEPT = v);
                LoadUrl("AMDEP", "API GET MASTER DEPT", v => rtn.UrlMasterDEPT = v);
                LoadUrl("ASKAT", "API GET STATUS KATEGORI", v => rtn.UrlStatusKATEGORI = v);
                LoadUrl("AMKAT", "API GET MASTER KATEGORI", v => rtn.UrlMasterKATEGORI = v);
                LoadUrl("ASKRT", "API GET STATUS PLU KRAT", v => rtn.UrlStatusPluKrat = v);
                LoadUrl("AMKRT", "API GET MASTER PLU KRAT", v => rtn.UrlMasterPluKrat = v);
                LoadUrl("ASTATP", "API GET STATUS TAT PRD", v => rtn.UrlStatusTATPrd = v);
                LoadUrl("AMTATP", "API GET MASTER TAT PRD", v => rtn.UrlMasterTATPrd = v);
                LoadUrl("APIPT", "URL API PERSEDIAAN TOKO", v => rtn.UrlApiPersediaanToko = v);
                LoadUrl("USERPT", "USERNAME API PERSEDIAAN TOKO", v => rtn.UsernameApiPersediaanToko = v);
                LoadUrl("PASSPT", "PASSWORD API PERSEDIAAN TOKO", v => rtn.PasswordApiPersediaanToko = v);

                // --- SMPKPS ---
                if (CountKey("SMPKPS") == 0)
                {
                    InsertIgnore("SMPKPS", "false", "SEND MASTER PRODUK KHUSUS KE API PERSEDIAAN TOKO", "");
                    rtn.SendMastProdSus = false;
                }
                else
                {
                    rtn.SendMastProdSus = GetNilai("SMPKPS")?.Trim().ToUpper() == "TRUE";
                }

                LoadUrl("ASTTT", "API GET STATUS TTT", v => rtn.UrlStatusTTT = v);
                LoadUrl("AMTTT", "API GET MASTER TTT", v => rtn.UrlMasterTTT = v);
                LoadUrl("ASTAT", "API GET STATUS TAT", v => rtn.UrlStatusTAT = v);
                LoadUrl("AMTAT", "API GET MASTER TAT", v => rtn.UrlMasterTAT = v);
                LoadUrl("ASTPS", "API GET STATUS TOKO_PRODSUS", v => rtn.UrlStatusTokoProdSus = v);
                LoadUrl("AMTPS", "API GET MASTER TOKO_PRODSUS", v => rtn.UrlMasterTokoProdSus = v);
                LoadUrl("ASPJT", "API GET STATUS PRODSUS_JUAL_TOKO", v => rtn.UrlStatusProdSusJualToko = v);
                LoadUrl("AMPJT", "API GET MASTER PRODSUS_JUAL_TOKO", v => rtn.UrlMasterProdSusJualToko = v);
                LoadUrl("ASTPP", "API GET STATUS TOKO_PRODSUS_PERIODE", v => rtn.UrlStatusTokoProdSusPeriode = v);
                LoadUrl("AMTPP", "API GET MASTER TOKO_PRODSUS_PERIODE", v => rtn.UrlMasterTokoProdSusPeriode = v);
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql, Utility.TipeLog.Error);
                rtn = new Config();
            }

            return rtn;
        }
    }
}