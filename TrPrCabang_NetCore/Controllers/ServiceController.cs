using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using TrPrCabang_NetCore.Connection;
using TrPrCabang_NetCore.Models;
using TrPrCabang_NetCore.Services;

namespace TrPrCabang_NetCore.Controllers
{
    public class ServiceController
    {
        private readonly Utility _objUtil;
        private readonly CompressHelper _objComp;
        private readonly IDbServices db;
        private readonly HttpClient _httpClient;
        private readonly PosRtSoapService _posRtSoap;

        public ServiceController(HttpClient httpClient, Utility objUtil, CompressHelper objComp)
        {
            _httpClient = httpClient;
            _objUtil = objUtil;
            _objComp = objComp;
            _posRtSoap = new PosRtSoapService(httpClient, objUtil);
        }

        private async Task<string> FetchAndDecompress(string jobs, string webString, bool isDecompress, string logLabel)
        {
            string strReader = "";
            string results = "";
            double sizeReader = 0;

            try
            {
                var response = await _httpClient.GetAsync(webString);
                strReader = await response.Content.ReadAsStringAsync();

                if (isDecompress)
                {
                    sizeReader = Encoding.ASCII.GetByteCount(strReader) / 1024.0;
                    var valueOriginal = strReader.Replace("\"", "");
                    var inputBytes = Convert.FromBase64String(valueOriginal);
                    results = _objComp.Decompress(jobs, inputBytes);
                }
                else
                {
                    results = strReader;
                }
            }
            catch (HttpRequestException ex)
            {
                results = $"[ERROR_EX_WEB] {ex.Message}";
            }
            catch (Exception ex)
            {
                results = $"[ERROR_EX] {ex.Message}";
            }
            finally
            {
                var logReader = strReader.Length > 1000 ? strReader[..1000] + " ..." : strReader;
                var logResult = results.Length > 1000 ? results[..1000] + " ..." : results;

                _objUtil.Tracelog(jobs,
                    $"{logLabel}\r\n" +
                    $"URL         : {webString}\r\n" +
                    $"SREADER     :\r\n{logReader}\r\n" +
                    $"SIZE READER : {sizeReader} KB\r\n" +
                    $"RESULTS     :\r\n{logResult}",
                    Utility.TipeLog.Debug);

                _objUtil.TraceLogJSON(
                    $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {logLabel}\r\n" +
                    $"URL      : {webString}\r\n" +
                    $"RESPONSE : {results}\r\n");
            }

            return results;
        }

        // ── GetStatusService ──────────────────────────────────────────────────────
        public async Task<string> GetStatusService(string jobs, Config conf, string cabang, string jenis, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = jenis switch
                {
                    "PAJAK" => conf.UrlStatusPajak + cabang,
                    "SUPPLIER" => conf.UrlStatusSupplier + cabang,
                    "PRODUK" => conf.UrlStatusProduk,
                    "SARANA_PRODUK_KHUSUS" => conf.UrlStatusSaranaProduk + cabang,
                    "TKX" => conf.UrlStatusTKX + cabang,
                    "DCX" => conf.UrlStatusDCX + cabang,
                    "DIVISI" => conf.UrlStatusDIVISI,
                    "DEPT" => conf.UrlStatusDEPT,
                    "KATEGORI" => conf.UrlStatusKATEGORI,
                    "PLU_KRAT" => conf.UrlStatusPluKrat,
                    "TAT_PRD" => conf.UrlStatusTATPrd + cabang,
                    "TTT" => conf.UrlStatusTTT + $"?tproses='{DateTime.Now:dd-MMM-yyyy}'",
                    "TAT" => conf.UrlStatusTAT + $"?tproses='{DateTime.Now:dd-MMM-yyyy}'",
                    "TOKO_PRODSUS" => conf.UrlStatusTokoProdSus + cabang,
                    "PRODSUS_JUAL_TOKO" => conf.UrlStatusProdSusJualToko + cabang,
                    "TOKO_PRODSUS_PERIODE" => conf.UrlStatusTokoProdSusPeriode + cabang,
                    _ => conf.UrlStatusPromo + cabang + conf.DeliStatusPromo + jenis
                };

                return await FetchAndDecompress(jobs, webString, isDecompress,
                    $"[LOG STATUS]\r\nJENIS : {jenis}");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        // ── GetMasterPromosiService ───────────────────────────────────────────────
        public async Task<string> GetMasterPromosiService(string jobs, Config conf)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterPromo;
                return await FetchAndDecompress(jobs, webString, isDecompress: true,
                    "[LOG MASTER]\r\nMASTER : PROMO");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        // ── GetMasterPajakService ─────────────────────────────────────────────────
        public async Task<string> GetMasterPajakService(string jobs, Config conf)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterPajak + conf.KodeCabang;
                return await FetchAndDecompress(jobs, webString, isDecompress: true,
                    "[LOG MASTER]\r\nMASTER : PAJAK");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        // ── GetMasterSupplierService ──────────────────────────────────────────────
        public async Task<string> GetMasterSupplierService(string jobs, Config conf)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterSupplier + conf.KodeCabang;
                return await FetchAndDecompress(jobs, webString, isDecompress: true,
                    "[LOG MASTER]\r\nMASTER : SUPPLIER");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterProdukKhususService(string jobs, Config conf)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterProduk;
                return await FetchAndDecompress(jobs, webString, isDecompress: true,
                    "[LOG MASTER]\r\nMASTER : PRODUK KHUSUS");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterSaranaProdukKhususService(
            string jobs, string cabang, Config conf, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterSaranaProduk + cabang;
                return await FetchAndDecompress(jobs, webString, isDecompress,
                    "[LOG MASTER]\r\nMASTER : SARANA PRODUK KHUSUS");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterTTTService(string jobs, Config conf, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterTTT + $"?tproses='{DateTime.Now:dd-MMM-yyyy}'";
                return await FetchAndDecompress(jobs, webString, isDecompress,
                    "[LOG MASTER]\r\nMASTER : TTT");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterTATService(string jobs, Config conf, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterTAT + $"?tproses='{DateTime.Now:dd-MMM-yyyy}'";
                return await FetchAndDecompress(jobs, webString, isDecompress,
                    "[LOG MASTER]\r\nMASTER : TAT");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterTokoProdSusService(string jobs, Config conf, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterTokoProdSus + conf.KodeCabang;
                return await FetchAndDecompress(jobs, webString, isDecompress,
                    "[LOG MASTER]\r\nMASTER : TOKO_PRODSUS");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterProdSusJualTokoService(string jobs, Config conf, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterProdSusJualToko + conf.KodeCabang;
                return await FetchAndDecompress(jobs, webString, isDecompress,
                    "[LOG MASTER]\r\nMASTER : PRODSUS_JUAL_TOKO");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterTokoProdSusPeriodeService(string jobs, Config conf, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterTokoProdSusPeriode + conf.KodeCabang;
                return await FetchAndDecompress(jobs, webString, isDecompress,
                    "[LOG MASTER]\r\nMASTER : TOKO_PRODSUS_PERIODE");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetPartisipanService(string jobs, Config conf, string cabang, string jenis)
        {
            string webString = "";
            try
            {
                webString = jenis == "SUPPLIER"
                    ? conf.UrlPartisipanSupplier + cabang
                    : conf.UrlPartisipanPromo + cabang;

                return await FetchAndDecompress(jobs, webString, isDecompress: true,
                    $"[LOG PARTISIPAN]\r\nJENIS : {jenis}");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterTKXService(string jobs, Config conf, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterTKX + conf.KodeCabang;
                return await FetchAndDecompress(jobs, webString, isDecompress,
                    "[LOG MASTER]\r\nMASTER : TKX");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterDCXService(string jobs, Config conf, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterDCX + conf.KodeCabang;
                return await FetchAndDecompress(jobs, webString, isDecompress,
                    "[LOG MASTER]\r\nMASTER : DCX");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterDIV_DEPT_KAT_Service(string jobs, Config conf, string jenis, bool isDecompress)
        {
            string webString = "";
            try
            {
                var jenisUpper = jenis.ToUpper();
                if (jenisUpper == "DIVISI")
                    webString = conf.UrlMasterDIVISI;
                else if (jenisUpper == "DEPT")
                    webString = conf.UrlMasterDEPT;
                else if (jenisUpper == "KATEGORI")
                    webString = conf.UrlMasterKATEGORI;
                else
                    return $"Jenis Master tidak dikenali (Jenis:{jenis})";

                return await FetchAndDecompress(jobs, webString, isDecompress,
                    $"[LOG MASTER]\r\nMASTER : {jenisUpper}");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterTAT_PRD_Service(string jobs, Config conf, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterTATPrd + conf.KodeCabang;
                return await FetchAndDecompress(jobs, webString, isDecompress,
                    "[LOG MASTER]\r\nMASTER : TAT_PRD");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<string> GetMasterPLU_KRAT_Service(string jobs, Config conf, bool isDecompress)
        {
            string webString = "";
            try
            {
                webString = conf.UrlMasterPluKrat;
                return await FetchAndDecompress(jobs, webString, isDecompress,
                    "[LOG MASTER]\r\nMASTER : PLU_KRAT");
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nWebString: " + webString,
                    Utility.TipeLog.Error);
                return "";
            }
        }

        public async Task<bool> SendTrPrTask_Service(string jobs, byte[] sqlCmd, string toko, string judulTask, Config conf, Action<string> setMsg)
        {
            int countFailed = 0;
            double sizeCmd = sqlCmd.Length / 1024.0;

            do
            {
                try
                {
                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [DEBUG-SendTrPrTask_Service]\r\n" +
                                          $"{judulTask}-{toko}-SizeData: {sizeCmd} KB-Request Send.");

                    var responService = await _posRtSoap.InsertTrPrCabAsync(
                        conf.UrlPosRT_Service, judulTask, toko, sqlCmd);

                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [DEBUG-SendTrPrTask_Service]\r\n" +
                                          $"{judulTask}-{toko}-SizeData: {sizeCmd} KB-Respond Send: {responService}");

                    if (!string.IsNullOrWhiteSpace(responService) && responService.Contains("|"))
                    {
                        setMsg(responService);
                        return responService.Split('|')[0] == "00";
                    }

                    return false;
                }
                catch (Exception ex)
                {
                    countFailed++;
                    _objUtil.Tracelog(jobs,
                        ex.Message + "\r\n" + ex.StackTrace +
                        $"\r\nTaskName: {judulTask}\r\nCountFailed: {countFailed}",
                        Utility.TipeLog.Error);

                    if (countFailed == 1) await Task.Delay(1000);
                }
            } while (countFailed == 1);

            return false;
        }

        public async Task<bool> SendTrPrTask_RestApi(string jobs, string sqlCmd, string toko, string judulTask, Config conf, string setMsg)
        {
            int countFailed = 0;
            double sizeCmd = sqlCmd.Length / 1024.0;

            do
            {
                try
                {
                    var authHeader = Convert.ToBase64String(
                        Encoding.UTF8.GetBytes($"{conf.AuthUserPosRT_RestApi}:{conf.AuthPassPosRT_RestApi}"));

                    var queryEncode = _objComp.CompressAndEncodeString(sqlCmd);
                    var json = $"{{\"idClient\":\"{conf.KodeCabang}\",\"ipClient\":\"{conf.IpClient}\"," +
                               $"\"storeCode\":\"{toko}\",\"taskName\":\"{judulTask}\"," +
                               $"\"cmd\":\"{queryEncode}\",\"keterangan\":\"TrPrCabang\"}}";

                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [DEBUG-SendTrPrTask_RestApi]\r\n" +
                                          $"{toko} | {judulTask} | SizeData: {sizeCmd} KB | Request Send.");

                    var request = new HttpRequestMessage(HttpMethod.Post,
                        conf.UrlPosRT_RestApi + "/Branch/InsertTrPr")
                    {
                        Headers = { { "Authorization", "Basic " + authHeader } },
                        Content = new StringContent(json, Encoding.UTF8, "application/json")
                    };

                    var httpResponse = await _httpClient.SendAsync(request);
                    var responseApi = await httpResponse.Content.ReadAsStringAsync();

                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [DEBUG-SendTrPrTask_RestApi]\r\n" +
                                          $"{toko} | {judulTask} | SizeData: {sizeCmd} KB | Respond Send: {responseApi}");

                    var objResponseApi = JsonConvert.DeserializeObject<ResponseRestApi>(responseApi);
                    if (objResponseApi?.status == 200)
                        return true;

                    setMsg = objResponseApi?.message ?? "";
                    return false;
                }
                catch (HttpRequestException ex)
                {
                    countFailed++;
                    var errorMsg = ex.Message + "\r\n" + ex.StackTrace;

                    _objUtil.Tracelog(jobs,
                        $"{errorMsg}\r\nToko: {toko} | TaskName: {judulTask}\r\nCountFailed: {countFailed}",
                        Utility.TipeLog.Error);

                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [ERROR-SendTrPrTask_RestApi]\r\n" +
                                          $"{toko} | {judulTask} | CountFailed: {countFailed} | Error: {errorMsg}");

                    setMsg = errorMsg;
                    if (countFailed == 1)
                        await Task.Delay(1000);
                }
                catch (Exception ex)
                {
                    countFailed++;
                    var errorMsg = ex.Message + "\r\n" + ex.StackTrace;

                    _objUtil.Tracelog(jobs,
                        $"{errorMsg}\r\nToko: {toko} | TaskName: {judulTask}\r\nCountFailed: {countFailed}",
                        Utility.TipeLog.Error);

                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [ERROR-SendTrPrTask_RestApi]\r\n" +
                                          $"{toko} | {judulTask} | CountFailed: {countFailed} | Error: {errorMsg}");

                    setMsg = errorMsg;
                    if (countFailed == 1)
                        await Task.Delay(1000);
                }
            } while (countFailed == 1);

            return false;
        }

        public async Task<bool> ExecuteQueryPosrt_Service(string jobs, byte[] sqlCmd, Config conf, Action<string> setMsg)
        {
            int countFailed = 0;
            double sizeCmd = sqlCmd.Length / 1024.0;

            do
            {
                try
                {
                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [DEBUG-ExecuteQueryPosrt_Service]\r\n" +
                                          $"SizeData: {sizeCmd} KB-Request Send.");

                    var responService = await _posRtSoap.ExecuteQueryAsync(
                        conf.UrlPosRT_Service, sqlCmd);

                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [DEBUG-ExecuteQueryPosrt_Service]\r\n" +
                                          $"SizeData: {sizeCmd} KB-Respond Send: {responService}");

                    if (!string.IsNullOrWhiteSpace(responService) && responService.Contains("|"))
                    {
                        setMsg(responService);
                        return responService.Split('|')[0] == "00";
                    }

                    return false;
                }
                catch (Exception ex)
                {
                    countFailed++;
                    _objUtil.Tracelog(jobs,
                        ex.Message + "\r\n" + ex.StackTrace +
                        $"\r\nExecuteQueryPosrt_Service\r\nCountFailed: {countFailed}",
                        Utility.TipeLog.Error);

                    if (countFailed == 1) await Task.Delay(1000);
                }
            } while (countFailed == 1);

            return false;
        }

        public async Task<bool> ExecuteQueryPosrt_RestApi(string jobs, string sqlCmd, Config conf, Action<string> setMsg)
        {
            int countFailed = 0;

            do
            {
                try
                {
                    var authHeader = Convert.ToBase64String(
                        Encoding.UTF8.GetBytes($"{conf.AuthUserPosRT_RestApi}:{conf.AuthPassPosRT_RestApi}"));

                    var queryEncode = _objComp.CompressAndEncodeString(sqlCmd);
                    var json = $"{{\"idClient\":\"{conf.KodeCabang}\",\"ipClient\":\"{conf.IpClient}\"," +
                               $"\"taskQuery\":\"{queryEncode}\"}}";

                    double sizeCmd = json.Length / 1024.0;
                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [DEBUG-ExecuteQueryPosrt_RestApi]\r\n" +
                                          $"SizeData: {sizeCmd} KB-Request Send.");

                    var request = new HttpRequestMessage(HttpMethod.Post,
                        conf.UrlPosRT_RestApi + "/Branch/TaskExcQuery")
                    {
                        Headers = { { "Authorization", "Basic " + authHeader } },
                        Content = new StringContent(json, Encoding.UTF8, "application/json")
                    };

                    var httpResponse = await _httpClient.SendAsync(request);
                    var responseApi = await httpResponse.Content.ReadAsStringAsync();

                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [DEBUG-ExecuteQueryPosrt_RestApi]\r\n" +
                                          $"SizeData: {sizeCmd} KB-Respond Send: {responseApi}");

                    var objResponse = JsonConvert.DeserializeObject<ResponseRestApi>(responseApi);
                    if (objResponse?.status == 200)
                        return true;

                    setMsg(objResponse?.message ?? "");
                    return false;
                }
                catch (HttpRequestException ex)
                {
                    countFailed++;
                    var errorMsg = ex.Message + "\r\n" + ex.StackTrace;

                    _objUtil.Tracelog(jobs,
                        $"{errorMsg}\r\nExecuteQueryPosrt_RestApi\r\nCountFailed: {countFailed}",
                        Utility.TipeLog.Error);
                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [ERROR-ExecuteQueryPosrt_RestApi]\r\n" +
                                          $"CountFailed: {countFailed} | Error: {errorMsg}");

                    setMsg(errorMsg);
                    if (countFailed == 1) await Task.Delay(1000);
                }
                catch (Exception ex)
                {
                    countFailed++;
                    var errorMsg = ex.Message + "\r\n" + ex.StackTrace;

                    _objUtil.Tracelog(jobs,
                        $"{errorMsg}\r\nExecuteQueryPosrt_RestApi\r\nCountFailed: {countFailed}",
                        Utility.TipeLog.Error);
                    _objUtil.TraceLogFile($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: [ERROR-ExecuteQueryPosrt_RestApi]\r\n" +
                                          $"CountFailed: {countFailed} | Error: {errorMsg}");

                    setMsg(errorMsg);
                    if (countFailed == 1) await Task.Delay(1000);
                }
            } while (countFailed == 1);

            return false;
        }

        public async Task<bool> InsertTaskGzip_PersediaanToko(string jobs, string storeDest, string taskDesc, string taskFormat, string taskData, Config conf, Action<string> setMsg)
        {
            string url = conf.UrlApiPersediaanToko + "/TaskStore/InsertTaskGzip";
            try
            {
                var authHeader = Convert.ToBase64String(
                    Encoding.UTF8.GetBytes($"{conf.UsernameApiPersediaanToko}:{conf.PasswordApiPersediaanToko}"));

                // ── GZip compress + Base64 ────────────────────────────────────
                string dataAsBase64;
                using (var memoryStream = new MemoryStream())
                {
                    using (var gzipStream = new GZipStream(memoryStream, CompressionMode.Compress))
                    {
                        var dataBytes = Encoding.UTF8.GetBytes(taskData);
                        await gzipStream.WriteAsync(dataBytes, 0, dataBytes.Length);
                    }
                    dataAsBase64 = Convert.ToBase64String(memoryStream.ToArray());
                }

                var base64UrlSafe = HttpUtility.UrlEncode(dataAsBase64);
                var formData = $"user_id=TRPRCABANG" +
                               $"&store_dest={storeDest}" +
                               $"&task_desc={taskDesc}" +
                               $"&task_format={taskFormat}" +
                               $"&task_data={base64UrlSafe}";

                var request = new HttpRequestMessage(HttpMethod.Post, url)
                {
                    Headers = { { "Authorization", "Basic " + authHeader } },
                    Content = new StringContent(formData, Encoding.ASCII, "application/x-www-form-urlencoded")
                };

                var httpResponse = await _httpClient.SendAsync(request);
                var responseStr = await httpResponse.Content.ReadAsStringAsync();

                var objResponse = JsonConvert.DeserializeObject<PERSEDIAAN_TOKO>(responseStr);
                if (objResponse?.status == 200)
                    return true;

                setMsg(objResponse?.message ?? "");
                return false;
            }
            catch (Exception ex)
            {
                setMsg(ex.Message);
                _objUtil.Tracelog(jobs,
                    $"{ex.Message}\r\n{ex.StackTrace}\r\n" +
                    $"URL        : {url}\r\n" +
                    $"StoreDest  : {storeDest}\r\n" +
                    $"TaskDesc   : {taskDesc}\r\n" +
                    $"TaskFormat : {taskFormat}",
                    Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<ClsRespon> GetTblMasterTokoService(string jenisMaster, string kdCabang)
        {
            var respon = new ClsRespon();
            string UrlMasterToko = string.Empty;
            try
            {
                var timeStamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");

                var data = (!string.IsNullOrWhiteSpace(kdCabang) && jenisMaster == "TOKO_EXTENDED")
                    ? $"{{\r\nJENIS_MASTER:\"{jenisMaster}\",\r\nUSER:\"test\",\r\nTIME_STAMP:\"{timeStamp}\",\r\nCABANG:\"{kdCabang}\"\r\n}}"
                    : $"{{\r\nJENIS_MASTER:\"{jenisMaster}\",\r\nUSER:\"test\",\r\nTIME_STAMP:\"{timeStamp}\"\r\n}}";

                var signature = AesEncrypt("test" + timeStamp + jenisMaster, "keysimulasi123");

                _objUtil.Tracelog("GET",
                    $"Get Master {jenisMaster} | {kdCabang}\r\n" +
                    $"URL: {UrlMasterToko}\r\n" +
                    $"JSON:\r\n{data}\r\n" +
                    $"Signature:\r\n{signature}\r\n" +
                    $"Timeout: {120 * 1000}",
                    Utility.TipeLog.Debug);

                var request = new HttpRequestMessage(HttpMethod.Post, UrlMasterToko)
                {
                    Headers = { { "Signature", signature } },
                    Content = new StringContent(data, Encoding.UTF8, "application/json")
                };

                // Timeout per-request via CancellationToken
                using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(120));
                var httpResponse = await _httpClient.SendAsync(request, cts.Token);
                var responseWs = await httpResponse.Content.ReadAsStringAsync();

                respon = JsonConvert.DeserializeObject<ClsRespon>(responseWs) ?? respon;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog("GET",
                    $"{jenisMaster} | {kdCabang} | ERROR : {ex.Message}\r\n{ex.StackTrace}",
                    Utility.TipeLog.Error);
            }
            return respon;
        }

        // ── Helpers ───────────────────────────────────────────────────────────────

        private static string AesEncrypt(string input, string pass)
        {
            // Pad key hingga minimal 32 karakter
            while (pass.Length < 32)
                pass += pass;

            var key = Encoding.ASCII.GetBytes(pass[..32]);
            var iv = Encoding.ASCII.GetBytes(pass[..16]);

            using var aes = System.Security.Cryptography.Aes.Create();
            aes.KeySize = 256;
            aes.BlockSize = 128;
            aes.Key = key;
            aes.IV = iv;
            aes.Mode = System.Security.Cryptography.CipherMode.CBC;

            var buffer = Encoding.ASCII.GetBytes(input);
            var encrypted = aes.CreateEncryptor().TransformFinalBlock(buffer, 0, buffer.Length);
            var base64 = Convert.ToBase64String(encrypted);

            return EncodeTo64(base64);
        }

        private static string EncodeTo64(string toEncode)
        {
            var bytes = Encoding.ASCII.GetBytes(toEncode);
            return Convert.ToBase64String(bytes)
                .Replace('+', '-')
                .Replace('/', '_');
        }

    }
}
