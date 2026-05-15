using Dapper;
using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TrPrCabang_NetCore.Connection;
using TrPrCabang_NetCore.Controllers;
using TrPrCabang_NetCore.Models;

namespace TrPrCabang_NetCore.DataAccess
{
    public class TrPrDA
    {
        private readonly Utility _objUtil;
        private readonly ServiceController _objSvr;
        private readonly CompressHelper _objComp;
        private readonly IDbServices db;

        public record PromoGroupDto(string KodePromo, int CountId);
        public record DraftPromoDto(string KdDc, string KdToko, string KdPromo);
        public record DraftSupplierDto(string KdCab, string KdTk, string SupCo);
        public record MasterPromoDto(string KodePromo);


        public TrPrDA(IDbServices db)
        {
            this.db = db;
        }

        public async Task<List<string>> GetCabang(string jobs)
        {
            var result = new List<string>();
            try
            {
                using var conn = db.CreateConnection();
                var data = await conn.QueryAsync<string>(
                    $"SELECT `kdcabang` FROM `poscabang`.`cabang`;");

                result = data.ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                result = new List<string>();
            }
            return result;
        }

        public async Task<bool> CekStatusLogMaster(string jobs, string jenis, DateTime tglProses)
        {
            bool rtn = false;
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                sql = $"SELECT TGL_PROSES FROM `{db.DatabaseName()}`.`log_master`" +
                      $" WHERE JENIS = @jenis" +
                      $" ORDER BY TGL_PROSES DESC LIMIT 1;";

                var lastTgl = await conn.ExecuteScalarAsync<DateTime?>(sql, new { jenis });
                if (lastTgl.HasValue)
                    rtn = lastTgl.Value >= tglProses;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                rtn = false;
            }
            return rtn;
        }

        public async Task<bool> CekStatusLogMaster_V2(string jobs, string jenis, DateTime tglStatus)
        {
            bool rtn = false;
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                var tglDate = tglStatus.Date;

                sql = $"SELECT TGL_STATUS FROM `{db.DatabaseName()}`.`log_master`" +
                      $" WHERE JENIS = @jenis" +
                      $" ORDER BY TGL_STATUS DESC LIMIT 1;";

                var lastTgl = await conn.ExecuteScalarAsync<DateTime?>(sql, new { jenis });
                if (lastTgl.HasValue)
                    rtn = lastTgl.Value.Date >= tglDate;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                rtn = false;
            }
            return rtn;
        }

        public async Task<List<string>> GetListKodeTokoByLogMaster(string jobs)
        {
            var result = new List<string>();
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                sql = $"SELECT DISTINCT KODETOKO FROM `{db.DatabaseName()}`.`log_master`;";
                var data = await conn.QueryAsync<string>(sql);
                result = data.ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
            }
            return result;
        }

        public async Task<List<string>> GetListKodeTokoExtended(string jobs, string kodeGudang)
        {
            var result = new List<string>();
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                sql = $"SELECT DISTINCT KODETOKO FROM `{db.DatabaseName()}`.`toko_extended`" +
                      $" WHERE `KodeGudang` = @kodeGudang;";
                var data = await conn.QueryAsync<string>(sql, new { kodeGudang });
                result = data.ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
            }
            return result;
        }

        public async Task<List<string>> GetListPromoToko(string jobs)
        {
            var result = new List<string>();
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                sql = $"SELECT DISTINCT KD_TOKO FROM `{db.DatabaseName()}`.`partisipan_promo`;";
                var data = await conn.QueryAsync<string>(sql);
                result = data.ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
            }
            return result;
        }

        public async Task<List<PromoGroupDto>> GetListPromoGroupID(string jobs)
        {
            var result = new List<PromoGroupDto>();
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                sql = $"SELECT KODEPROMO, COUNT(ID) AS COUNTID" +
                      $" FROM `{db.DatabaseName()}`.`master_all_promo`" +
                      $" GROUP BY KODEPROMO HAVING COUNT(ID) > 1;";
                var data = await conn.QueryAsync<PromoGroupDto>(sql);
                result = data.ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
            }
            return result;
        }

        public async Task<List<string>> GetListSupplierToko(string jobs)
        {
            var result = new List<string>();
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                sql = $"SELECT DISTINCT KDTK FROM `{db.DatabaseName()}`.`partisipan_supplier`;";
                var data = await conn.QueryAsync<string>(sql);
                result = data.ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
            }
            return result;
        }

        public async Task<List<dynamic>> GetDraftPartisipanMaster(string jenis, string jobs, string tglStatus, string tglProses, List<PromoGroupDto> listPromoGroupId, string listStrToko)
        {
            var result = new List<dynamic>();
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                var db_ = db.DatabaseName();

                if (jenis.ToUpper() == "PROMO")
                {
                    // ── Build query log_master ────────────────────────────────
                    var sqlLog = new StringBuilder();
                    sqlLog.Append($"SELECT JENIS, TGL_STATUS, CAST(DATE_FORMAT(TGL_PROSES,'%Y-%m-%d %H:%i:%s') AS CHAR) AS TGL_PROSES, KODETOKO, IDTOKO");
                    sqlLog.Append($" FROM `{db_}`.`log_master` WHERE JENIS = 'PROMO'");
                    if (!string.IsNullOrWhiteSpace(listStrToko))
                        sqlLog.Append($" AND KODETOKO IN ({listStrToko})");
                    if (!string.IsNullOrWhiteSpace(tglStatus))
                        sqlLog.Append($" AND TGL_STATUS = '{tglStatus}'");
                    if (!string.IsNullOrWhiteSpace(tglProses))
                        sqlLog.Append($" AND TGL_PROSES = '{tglProses}'");
                    if (jobs.ToUpper() == "GET")
                        sqlLog.Append(" AND `GET` = 'OK';");
                    else if (jobs.ToUpper() == "SEND")
                        sqlLog.Append(" AND `SEND` IS NULL ORDER BY TGL_PROSES DESC;");

                    sql = sqlLog.ToString();
                    var dtLog = (await conn.QueryAsync<dynamic>(sql)).ToList();

                    if (dtLog.Count > 0)
                    {
                        // Build promo group exclusion list
                        var strPromoGroup = listPromoGroupId.Count > 0
                            ? string.Join(",", listPromoGroupId.Select(x => $"'{x.KodePromo}'"))
                            : "";

                        await conn.ExecuteAsync("SET SESSION group_concat_max_len = 100000000;");

                        foreach (var dr in dtLog)
                        {
                            try
                            {
                                var sqlDraft = new StringBuilder();
                                sqlDraft.Append($"SELECT l.IDTOKO, p.KD_TOKO, l.TGL_STATUS, l.TGL_PROSES,");
                                sqlDraft.Append($" GROUP_CONCAT(CONCAT('''',p.KD_PROMO,'''')) AS KD_PROMO");
                                sqlDraft.Append($" FROM `{db_}`.`partisipan_promo` p");
                                sqlDraft.Append($" INNER JOIN (SELECT JENIS,TGL_STATUS,TGL_PROSES,KODETOKO,IDTOKO FROM `{db_}`.`log_master`");
                                sqlDraft.Append($" WHERE JENIS = '{jenis}'");
                                sqlDraft.Append($" AND IDTOKO = '{dr.IDTOKO}'");
                                if (!string.IsNullOrWhiteSpace(tglStatus))
                                    sqlDraft.Append($" AND TGL_STATUS = '{tglStatus}'");
                                if (!string.IsNullOrWhiteSpace(tglProses))
                                    sqlDraft.Append($" AND TGL_PROSES = '{tglProses}'");
                                else
                                    sqlDraft.Append($" AND TGL_PROSES = '{dr.TGL_PROSES}'");
                                if (jobs.ToUpper() == "GET")
                                    sqlDraft.Append(" AND `GET` = 'OK') l");
                                else if (jobs.ToUpper() == "SEND")
                                    sqlDraft.Append(" AND `SEND` IS NULL) l");
                                sqlDraft.Append(" ON p.KD_TOKO = l.KODETOKO WHERE 1=1");
                                if (!string.IsNullOrWhiteSpace(strPromoGroup))
                                    sqlDraft.Append($" AND p.KD_PROMO NOT IN ({strPromoGroup})");
                                sqlDraft.Append(" AND p.TGL_PROSES = l.TGL_PROSES");
                                sqlDraft.Append(" GROUP BY l.IDTOKO, p.KD_TOKO;");

                                sql = sqlDraft.ToString();
                                var rows = await conn.QueryAsync<dynamic>(sql);
                                result.AddRange(rows);
                            }
                            catch (Exception ex)
                            {
                                _objUtil.Tracelog(jobs,
                                    $"GetDraft Partisipan Promosi Toko Gagal (TOKO:{dr.KODETOKO}|TGL_PROSES:{dr.TGL_PROSES}).\r\n" +
                                    $"ERROR : {ex.Message}\r\n{ex.StackTrace}\r\nSQL: {sql}",
                                    Utility.TipeLog.Error);
                            }
                        }
                    }
                }
                else if (jenis.ToUpper() == "SUPPLIER")
                {
                    var sqlDraft = new StringBuilder();
                    sqlDraft.Append($"SELECT l.IDTOKO, p.KDTK, l.TGL_STATUS,");
                    sqlDraft.Append($" GROUP_CONCAT(CONCAT('''',p.SUPCO,'''')) AS SUPCO");
                    sqlDraft.Append($" FROM `{db_}`.`partisipan_supplier` p");
                    sqlDraft.Append($" INNER JOIN (SELECT JENIS,TGL_STATUS,TGL_PROSES,KODETOKO,IDTOKO FROM `{db_}`.`log_master`");
                    sqlDraft.Append($" WHERE JENIS = '{jenis}'");
                    if (!string.IsNullOrWhiteSpace(listStrToko))
                        sqlDraft.Append($" AND KODETOKO IN ({listStrToko})");
                    if (!string.IsNullOrWhiteSpace(tglStatus))
                        sqlDraft.Append($" AND TGL_STATUS = '{tglStatus}'");
                    if (!string.IsNullOrWhiteSpace(tglProses))
                        sqlDraft.Append($" AND TGL_PROSES = '{tglProses}'");
                    if (jobs.ToUpper() == "GET")
                        sqlDraft.Append(" AND `GET` = 'OK') l");
                    else if (jobs.ToUpper() == "SEND")
                        sqlDraft.Append(" AND `SEND` IS NULL) l");
                    sqlDraft.Append(" ON p.KDTK = l.KODETOKO WHERE 1=1");
                    sqlDraft.Append(" AND p.TGL_PROSES = l.TGL_PROSES");
                    sqlDraft.Append(" GROUP BY l.IDTOKO, p.KDTK;");

                    sql = sqlDraft.ToString();
                    await conn.ExecuteAsync("SET SESSION group_concat_max_len = 100000000;");
                    result = (await conn.QueryAsync<dynamic>(sql)).ToList();
                }
                else if (jenis.ToUpper() == "PAJAK")
                {
                    var sqlDraft = new StringBuilder();
                    sqlDraft.Append($"SELECT l.IDTOKO, p.*");
                    sqlDraft.Append($" FROM `{db_}`.`partisipan_pajak` p");
                    sqlDraft.Append($" INNER JOIN (SELECT JENIS,TGL_STATUS,TGL_PROSES,KODETOKO,IDTOKO FROM `{db_}`.`log_master`");
                    sqlDraft.Append($" WHERE JENIS = '{jenis}'");
                    if (!string.IsNullOrWhiteSpace(listStrToko))
                        sqlDraft.Append($" AND KODETOKO IN ({listStrToko})");
                    if (!string.IsNullOrWhiteSpace(tglStatus))
                        sqlDraft.Append($" AND TGL_STATUS = '{tglStatus}'");
                    if (!string.IsNullOrWhiteSpace(tglProses))
                        sqlDraft.Append($" AND TGL_PROSES = '{tglProses}'");
                    if (jobs.ToUpper() == "GET")
                        sqlDraft.Append(" AND `GET` = 'OK') l");
                    else if (jobs.ToUpper() == "SEND")
                        sqlDraft.Append(" AND `SEND` IS NULL) l");
                    sqlDraft.Append(" ON p.TOK_CODE = l.KODETOKO WHERE 1=1");
                    sqlDraft.Append(" AND p.TGL_PROSES = l.TGL_PROSES");
                    sqlDraft.Append(" GROUP BY l.IDTOKO, p.TOK_CODE;");

                    sql = sqlDraft.ToString();
                    result = (await conn.QueryAsync<dynamic>(sql)).ToList();
                }
                else
                {
                    // SARANA_PRODUK_KHUSUS | PRODUK_KHUSUS | TAT | TTT — pola sama
                    var jenisUpper = jenis.ToUpper();
                    var sendFilter = (jenisUpper == "SARANA_PRODUK_KHUSUS")
                        ? "`SEND` IS NULL"
                        : "(`SEND` IS NULL OR `SEND` = '')";

                    var sqlDraft = new StringBuilder();
                    sqlDraft.Append($"SELECT `KODETOKO`, `IDTOKO`,");
                    sqlDraft.Append($" CAST(DATE_FORMAT(`TGL_STATUS`,'%Y-%m-%d') AS CHAR) AS `TGL_STATUS`,");
                    sqlDraft.Append($" CAST(DATE_FORMAT(`TGL_PROSES`,'%Y-%m-%d %H:%i:%s') AS CHAR) AS `TGL_PROSES`");
                    sqlDraft.Append($" FROM `{db_}`.`log_master`");
                    sqlDraft.Append($" WHERE `JENIS` = '{jenisUpper}'");
                    if (!string.IsNullOrWhiteSpace(listStrToko) && jenisUpper == "SARANA_PRODUK_KHUSUS")
                        sqlDraft.Append($" AND KODETOKO IN ({listStrToko})");
                    sqlDraft.Append($" AND {sendFilter} ORDER BY `AddTime` DESC;");

                    sql = sqlDraft.ToString();
                    result = (await conn.QueryAsync<dynamic>(sql)).ToList();
                }
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                result = new List<dynamic>();
            }
            return result;
        }

        public async Task<List<DraftPromoDto>> GetDraftPartisipanPromo(string jobs, List<PromoGroupDto> listPromoGroupId)
        {
            var result = new List<DraftPromoDto>();
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                await conn.ExecuteAsync("SET SESSION group_concat_max_len = 100000000;");

                var sqlDraft = new StringBuilder();
                sqlDraft.Append($"SELECT KD_DC, KD_TOKO, GROUP_CONCAT(CONCAT('''',KD_PROMO,'''')) AS KD_PROMO");
                sqlDraft.Append($" FROM `{db.DatabaseName()}`.`partisipan_promo`");

                if (listPromoGroupId.Count > 0)
                {
                    var strPromoGroup = string.Join(",", listPromoGroupId.Select(x => $"'{x.KodePromo}'"));
                    sqlDraft.Append($" WHERE KD_PROMO NOT IN ({strPromoGroup})");
                }

                sqlDraft.Append(" GROUP BY KD_DC, KD_TOKO;");
                sql = sqlDraft.ToString();

                result = (await conn.QueryAsync<DraftPromoDto>(sql)).ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
            }
            return result;
        }

        public async Task<List<DraftSupplierDto>> GetDraftPartisipanSupplier(string jobs)
        {
            var result = new List<DraftSupplierDto>();
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                await conn.ExecuteAsync("SET SESSION group_concat_max_len = 100000000;");

                sql = $"SELECT KDCAB, KDTK, GROUP_CONCAT(CONCAT('''',SUPCO,'''')) AS SUPCO" +
                      $" FROM `{db.DatabaseName()}`.`partisipan_supplier`" +
                      $" GROUP BY KDCAB, KDTK;";

                result = (await conn.QueryAsync<DraftSupplierDto>(sql)).ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
            }
            return result;
        }

        public async Task<List<MasterPromoDto>> GetPromoTokoByKode(string jobs, string toko, string listKodePromo, List<PromoGroupDto> listPromoGroupId)
        {
            var result = new List<MasterPromoDto>();
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                var db_ = db.DatabaseName();

                // ── Query 1: ambil master promo by list kode ─────────────────
                sql = $"SELECT * FROM `{db_}`.`master_all_promo` WHERE KODEPROMO IN ({listKodePromo});";
                result = (await conn.QueryAsync<MasterPromoDto>(sql)).ToList();

                // ── Query 2: jika ada promo group ID > 1 ─────────────────────
                if (listPromoGroupId.Count > 0)
                {
                    var strPromoGroup = string.Join(",", listPromoGroupId.Select(x => $"'{x.KodePromo}'"));

                    sql = $"SELECT DISTINCT KD_PROMO FROM `{db_}`.`partisipan_promo`" +
                          $" WHERE KD_TOKO = @toko AND KD_PROMO IN ({strPromoGroup});";

                    var groupPromos = (await conn.QueryAsync<string>(sql, new { toko })).ToList();

                    // ── Query 3: ambil master promo per group promo ───────────
                    foreach (var kdPromo in groupPromos)
                    {
                        sql = $"SELECT * FROM `{db_}`.`master_all_promo` WHERE KODEPROMO = @kdPromo;";
                        var rows = await conn.QueryAsync<MasterPromoDto>(sql, new { kdPromo });
                        result.AddRange(rows);
                    }
                }
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                result = new List<MasterPromoDto>();
            }
            return result;
        }

        public async Task<List<dynamic>> GetSupplierTokoByKode(string jobs, string listSupco)
        {
            var result = new List<dynamic>();
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                sql = $"SELECT * FROM `{db.DatabaseName()}`.`master_all_supplier` WHERE SUPCO IN ({listSupco});";
                result = (await conn.QueryAsync<dynamic>(sql)).ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
            }
            return result;
        }

        public async Task<bool> InitialTables(string jobs)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                var db_ = db.DatabaseName();

                // ── Cek & buat schema ─────────────────────────────────────────
                var schema = await conn.ExecuteScalarAsync<string>($"SHOW SCHEMAS LIKE '{db_}';");
                if (schema == null)
                    await conn.ExecuteAsync("CREATE SCHEMA `trpr_cabang`;");

                var schema2 = await conn.ExecuteScalarAsync<string>("SHOW SCHEMAS LIKE 'trprservice';");
                if (schema2 == null)
                    await conn.ExecuteAsync("CREATE SCHEMA `trprservice`;");

                // ── tracelog_get ──────────────────────────────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`tracelog_get` (" +
                      " `IdTracelog` BIGINT(20) NOT NULL AUTO_INCREMENT," +
                      " `Tgl` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                      " `Tipe` VARCHAR(10) DEFAULT NULL COMMENT '1=info,2=warning,3=error,4=debug'," +
                      " `AppName` VARCHAR(50) DEFAULT NULL," +
                      " `Log` TEXT," +
                      " `AddId` VARCHAR(50) DEFAULT NULL," +
                      " PRIMARY KEY (`IdTracelog`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                // ── tracelog_send ─────────────────────────────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`tracelog_send` (" +
                      " `IdTracelog` BIGINT(20) NOT NULL AUTO_INCREMENT," +
                      " `Tgl` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP," +
                      " `Tipe` VARCHAR(10) DEFAULT NULL COMMENT '1=info,2=warning,3=error,4=debug'," +
                      " `AppName` VARCHAR(50) DEFAULT NULL," +
                      " `Log` TEXT," +
                      " `AddId` VARCHAR(50) DEFAULT NULL," +
                      " PRIMARY KEY (`IdTracelog`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                // ── Drop trpr_promo jika ada ──────────────────────────────────
                var tblPro = await conn.ExecuteScalarAsync<string>($"USE `{db_}`; SHOW TABLES LIKE 'trpr_promo';");
                if (!string.IsNullOrEmpty(tblPro))
                    await conn.ExecuteAsync($"DROP TABLE `{db_}`.`trpr_promo`;");

                // ── master_all_promo ──────────────────────────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`master_all_promo` (" +
                      " `KODEPROMO` varchar(10) NOT NULL DEFAULT ''," +
                      " `SUBKODEPROMO` varchar(45) NOT NULL DEFAULT ''," +
                      " `TIPEPROMO` varchar(45) DEFAULT NULL," +
                      " `KODEGROUP` varchar(10) NOT NULL DEFAULT ''," +
                      " `MEKANISME` varchar(2000) DEFAULT NULL," +
                      " `CETAKSTRUK1` varchar(45) DEFAULT NULL," +
                      " `CETAKSTRUK2` varchar(45) DEFAULT NULL," +
                      " `CETAKSTRUK3` varchar(45) DEFAULT NULL," +
                      " `CETAKLAYAR1` varchar(45) DEFAULT NULL," +
                      " `CETAKLAYAR2` varchar(45) DEFAULT NULL," +
                      " `CETAKLAYAR3` varchar(45) DEFAULT NULL," +
                      " `TANGGALAWAL` varchar(45) DEFAULT NULL," +
                      " `TANGGALAKHIR` varchar(45) DEFAULT NULL," +
                      " `PERIODEJAM` varchar(45) DEFAULT NULL," +
                      " `PERIODEMINGGUAN` varchar(45) DEFAULT NULL," +
                      " `PERIODEBULANAN` varchar(45) DEFAULT NULL," +
                      " `ITEMSYARAT` longtext," +
                      " `QTYSYARATMIN` varchar(45) DEFAULT NULL," +
                      " `QTYSYARATMAX` varchar(45) DEFAULT NULL," +
                      " `QTYSYARATTAMBAH` varchar(45) DEFAULT NULL," +
                      " `RPSYARATMIN` varchar(45) DEFAULT NULL," +
                      " `RPSYARATMAX` varchar(45) DEFAULT NULL," +
                      " `RPSYARATTAMBAH` varchar(45) DEFAULT NULL," +
                      " `ITEMTARGET` longtext," +
                      " `QTYTARGET` longtext," +
                      " `QTYTARGETMAX` longtext," +
                      " `QTYTARGETTAMBAH` longtext," +
                      " `RPTARGET` longtext," +
                      " `RPTARGETMAX` longtext," +
                      " `RPTARGETTAMBAH` longtext," +
                      " `POTONGANPERSENTARGET` longtext," +
                      " `KODEBIN` longtext," +
                      " `KODEMEMBER` longtext," +
                      " `BERSYARAT` varchar(1) DEFAULT NULL," +
                      " `KODESHIFT` varchar(1) DEFAULT NULL," +
                      " `NOSTRUK` varchar(45) DEFAULT NULL," +
                      " `SYARATCAMPUR` varchar(45) DEFAULT NULL," +
                      " `TRANSAKSIMAX` longtext," +
                      " `QTYTARGETMAXPROMO` longtext," +
                      " `RPTARGETMAXPROMO` longtext," +
                      " `TARGETTDKDIJUAL` longtext," +
                      " `STICKER` varchar(1) DEFAULT NULL," +
                      " `PEMBATASAN_HADIAH` varchar(1) DEFAULT NULL," +
                      " `TENGGANGWAKTU` longtext," +
                      " `QTYMAXPERSTRUK` longtext," +
                      " `HIGHIMPACT` varchar(3) DEFAULT NULL," +
                      " `POTONGMARGIN` varchar(500) DEFAULT NULL," +
                      " `MAXRPBELANJA` varchar(45) DEFAULT NULL," +
                      " `TGL_UPDATE` varchar(45) DEFAULT NULL," +
                      " `NOURUT` bigint(20) DEFAULT NULL," +
                      " `QTY_TBS_MRH` longtext," +
                      " `KEYPROMOSI` varchar(500) DEFAULT ''," +
                      " `IKIOS` varchar(1) DEFAULT ''," +
                      " `PRIORITAS` longtext," +
                      " `ID` DECIMAL(2,0) DEFAULT '1'," +
                      " `I_STORE` varchar(1) DEFAULT ''," +
                      " `SPECIAL_PRODUCT` varchar(1) DEFAULT ''," +
                      " `PRO_SPECIAL_REQUEST` varchar(45) DEFAULT ''," +
                      " `PRO_JENIS_SPECIAL_REQ` varchar(45) DEFAULT ''," +
                      " `PRO_EXCLUDE_BIN` longtext," +
                      " `HARIED` varchar(20) DEFAULT ''," +
                      " `PRO_DESC_PRICE_TAG` varchar(30) DEFAULT ''," +
                      " `PRO_JENIS_SHELFTALKER` varchar(50) DEFAULT ''," +
                      " `UPDID_CABANG` varchar(45) DEFAULT NULL," +
                      " `UPDTIME_CABANG` datetime DEFAULT NULL," +
                      " PRIMARY KEY (`KODEPROMO`,`SUBKODEPROMO`,`KODEGROUP`,`ID`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                // Alter master_all_promo — cek kolom satu per satu
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "QTY_TBS_MRH",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `QTY_TBS_MRH` longtext AFTER `NOURUT`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "KEYPROMOSI",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `KEYPROMOSI` varchar(500) DEFAULT '' AFTER `QTY_TBS_MRH`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "IKIOS",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `IKIOS` varchar(1) DEFAULT '' AFTER `KEYPROMOSI`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "PRIORITAS",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `PRIORITAS` longtext AFTER `IKIOS`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "ID",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `ID` DECIMAL(2,0) DEFAULT '1' AFTER `PRIORITAS`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "I_STORE",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `I_STORE` VARCHAR(1) DEFAULT '' AFTER `ID`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "SPECIAL_PRODUCT",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `SPECIAL_PRODUCT` VARCHAR(1) DEFAULT '' AFTER `I_STORE`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "PRO_SPECIAL_REQUEST",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `PRO_SPECIAL_REQUEST` VARCHAR(45) DEFAULT '' AFTER `SPECIAL_PRODUCT`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "PRO_JENIS_SPECIAL_REQ",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `PRO_JENIS_SPECIAL_REQ` VARCHAR(45) DEFAULT '' AFTER `PRO_SPECIAL_REQUEST`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "PRO_EXCLUDE_BIN",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `PRO_EXCLUDE_BIN` longtext AFTER `PRO_JENIS_SPECIAL_REQ`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "HARIED",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `HARIED` varchar(20) DEFAULT '' AFTER `PRO_EXCLUDE_BIN`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "PRO_DESC_PRICE_TAG",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `PRO_DESC_PRICE_TAG` varchar(30) DEFAULT '' AFTER `HARIED`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_promo", "PRO_JENIS_SHELFTALKER",
                    $"ALTER TABLE `{db_}`.`master_all_promo` ADD COLUMN `PRO_JENIS_SHELFTALKER` varchar(50) DEFAULT '' AFTER `PRO_DESC_PRICE_TAG`;");

                // Cek PK ID master_all_promo
                await AlterIfColumnNotExistsWithKey(conn, db_, "master_all_promo", "ID", "PRI",
                    $"ALTER TABLE `{db_}`.`master_all_promo` CHANGE `ID` `ID` DECIMAL(2,0) DEFAULT 1 NOT NULL, DROP PRIMARY KEY," +
                    $" ADD PRIMARY KEY (`KODEPROMO`, `SUBKODEPROMO`, `KODEGROUP`, `ID`);");

                // ── partisipan_promo ──────────────────────────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`partisipan_promo` (" +
                      " `KD_DC` VARCHAR(4) NOT NULL," +
                      " `KD_TOKO` VARCHAR(4) NOT NULL," +
                      " `KD_PROMO` VARCHAR(10) NOT NULL," +
                      " `NO_URUT` BIGINT(20) DEFAULT NULL," +
                      " `TGL_UPDATE` VARCHAR(45) DEFAULT NULL," +
                      " `TGL_STATUS` DATE NOT NULL," +
                      " `TGL_PROSES` DATETIME NOT NULL," +
                      " `UPDID_CABANG` VARCHAR(45) DEFAULT NULL," +
                      " `UPDTIME_CABANG` DATETIME DEFAULT NULL," +
                      " PRIMARY KEY (`KD_DC`,`KD_TOKO`,`KD_PROMO`)," +
                      " KEY `IDX_DC` (`KD_DC`)," +
                      " KEY `IDX_TOKO` (`KD_TOKO`)," +
                      " KEY `IDX_PROMO` (`KD_PROMO`)," +
                      " KEY `IDX_TGL_STATUS` (`TGL_STATUS`)," +
                      " KEY `IDX_TGL_PROSES` (`TGL_PROSES`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                await AlterIfColumnNotExists(conn, db_, "partisipan_promo", "TGL_STATUS",
                    $"ALTER TABLE `{db_}`.`partisipan_promo` ADD COLUMN `TGL_STATUS` DATE AFTER `TGL_UPDATE`;");
                await AlterIfIndexNotExists(conn, db_, "partisipan_promo", "TGL_STATUS",
                    $"ALTER TABLE `{db_}`.`partisipan_promo` ADD KEY `IDX_TGL_STATUS` (`TGL_STATUS`);");
                await AlterIfColumnNotExists(conn, db_, "partisipan_promo", "TGL_PROSES",
                    $"ALTER TABLE `{db_}`.`partisipan_promo` ADD COLUMN `TGL_PROSES` DATETIME AFTER `TGL_STATUS`;");
                await AlterIfIndexNotExists(conn, db_, "partisipan_promo", "TGL_PROSES",
                    $"ALTER TABLE `{db_}`.`partisipan_promo` ADD KEY `IDX_TGL_PROSES` (`TGL_PROSES`);");

                // ── partisipan_pajak ──────────────────────────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`partisipan_pajak` (" +
                      " `KD_DC` VARCHAR(4) NOT NULL," +
                      " `TOK_CODE` VARCHAR(4) NOT NULL," +
                      " `TOK_SKP` VARCHAR(100) NOT NULL," +
                      " `TOK_NAMA_FRC` VARCHAR(100) NOT NULL," +
                      " `TOK_NPWP` VARCHAR(100) NOT NULL," +
                      " `START_TGL_BERLAKU_SE` VARCHAR(100) NOT NULL," +
                      " `END_TGL_BERLAKU_SE` VARCHAR(100) NOT NULL," +
                      " `FLAG_PKP` VARCHAR(100) NOT NULL," +
                      " `TGL_STATUS` DATE NOT NULL," +
                      " `TGL_PROSES` DATETIME NOT NULL," +
                      " `UPDID_CABANG` VARCHAR(45) DEFAULT NULL," +
                      " `UPDTIME_CABANG` DATETIME DEFAULT NULL," +
                      " PRIMARY KEY (`KD_DC`,`TOK_CODE`)," +
                      " KEY `IDX_DC` (`KD_DC`)," +
                      " KEY `IDX_TOKO` (`TOK_CODE`)," +
                      " KEY `IDX_TGL_STATUS` (`TGL_STATUS`)," +
                      " KEY `IDX_TGL_PROSES` (`TGL_PROSES`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                await AlterIfColumnNotExists(conn, db_, "partisipan_pajak", "FLAG_PKP",
                    $"ALTER TABLE `{db_}`.`partisipan_pajak` ADD COLUMN `FLAG_PKP` VARCHAR(100) AFTER `END_TGL_BERLAKU_SE`;");
                await AlterIfColumnNotExists(conn, db_, "partisipan_pajak", "TGL_STATUS",
                    $"ALTER TABLE `{db_}`.`partisipan_pajak` ADD COLUMN `TGL_STATUS` DATE AFTER `FLAG_PKP`;");
                await AlterIfIndexNotExists(conn, db_, "partisipan_pajak", "TGL_STATUS",
                    $"ALTER TABLE `{db_}`.`partisipan_pajak` ADD KEY `IDX_TGL_STATUS` (`TGL_STATUS`);");
                await AlterIfColumnNotExists(conn, db_, "partisipan_pajak", "TGL_PROSES",
                    $"ALTER TABLE `{db_}`.`partisipan_pajak` ADD COLUMN `TGL_PROSES` DATETIME AFTER `TGL_STATUS`;");
                await AlterIfIndexNotExists(conn, db_, "partisipan_pajak", "TGL_PROSES",
                    $"ALTER TABLE `{db_}`.`partisipan_pajak` ADD KEY `IDX_TGL_PROSES` (`TGL_PROSES`);");

                // ── partisipan_supplier ───────────────────────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`partisipan_supplier` (" +
                      " `KDCAB` VARCHAR(4) NOT NULL," +
                      " `KDTK` VARCHAR(4) NOT NULL," +
                      " `SUPCO` VARCHAR(100) NOT NULL," +
                      " `LT` INTEGER NOT NULL," +
                      " `JADWAL` VARCHAR(100) NOT NULL," +
                      " `DATANG` VARCHAR(100) NOT NULL," +
                      " `KAPASITAS` INTEGER NOT NULL," +
                      " `FAX` VARCHAR(45) DEFAULT NULL," +
                      " `TGL_STATUS` DATE NOT NULL," +
                      " `TGL_PROSES` DATETIME NOT NULL," +
                      " `UPDTIME_CABANG` DATETIME DEFAULT NULL," +
                      " PRIMARY KEY (`KDCAB`,`KDTK`,`SUPCO`)," +
                      " KEY `IDX_DC` (`KDCAB`)," +
                      " KEY `IDX_TOKO` (`KDTK`)," +
                      " KEY `IDX_SUPCO` (`SUPCO`)," +
                      " KEY `IDX_TGL_STATUS` (`TGL_STATUS`)," +
                      " KEY `IDX_TGL_PROSES` (`TGL_PROSES`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                await AlterIfColumnNotExists(conn, db_, "partisipan_supplier", "TGL_STATUS",
                    $"ALTER TABLE `{db_}`.`partisipan_supplier` ADD COLUMN `TGL_STATUS` DATE AFTER `FAX`;");
                await AlterIfIndexNotExists(conn, db_, "partisipan_supplier", "TGL_STATUS",
                    $"ALTER TABLE `{db_}`.`partisipan_supplier` ADD KEY `IDX_TGL_STATUS` (`TGL_STATUS`);");
                await AlterIfColumnNotExists(conn, db_, "partisipan_supplier", "TGL_PROSES",
                    $"ALTER TABLE `{db_}`.`partisipan_supplier` ADD COLUMN `TGL_PROSES` DATETIME AFTER `TGL_STATUS`;");
                await AlterIfIndexNotExists(conn, db_, "partisipan_supplier", "TGL_PROSES",
                    $"ALTER TABLE `{db_}`.`partisipan_supplier` ADD KEY `IDX_TGL_PROSES` (`TGL_PROSES`);");

                // ── Drop master_supplier jika ada ─────────────────────────────
                var tblSup = await conn.ExecuteScalarAsync<string>($"USE `{db_}`; SHOW TABLES LIKE 'master_supplier';");
                if (!string.IsNullOrEmpty(tblSup))
                    await conn.ExecuteAsync($"DROP TABLE `{db_}`.`master_supplier`;");

                // ── master_all_supplier ───────────────────────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`master_all_supplier` (" +
                      " `KDCAB` VARCHAR(25) NOT NULL," +
                      " `SUPCO` VARCHAR(25) NOT NULL," +
                      " `NAMA` VARCHAR(50) DEFAULT NULL," +
                      " `ALMT1` VARCHAR(50) DEFAULT NULL," +
                      " `ALMT2` VARCHAR(50) DEFAULT NULL," +
                      " `ALMT3` VARCHAR(50) DEFAULT NULL," +
                      " `TELP1` VARCHAR(50) DEFAULT NULL," +
                      " `TELP2` VARCHAR(50) DEFAULT NULL," +
                      " `FAX` VARCHAR(50) DEFAULT NULL," +
                      " `TOP` VARCHAR(50) DEFAULT NULL," +
                      " `PKP` VARCHAR(50) DEFAULT NULL," +
                      " `NPWP` VARCHAR(50) DEFAULT NULL," +
                      " `TGL_SKP` VARCHAR(50) DEFAULT NULL," +
                      " `SKP` VARCHAR(50) DEFAULT NULL," +
                      " `DATANG` VARCHAR(50) DEFAULT NULL," +
                      " `DCONTINUE` VARCHAR(50) DEFAULT NULL," +
                      " `SS` VARCHAR(50) DEFAULT NULL," +
                      " `LT` VARCHAR(50) DEFAULT NULL," +
                      " `JADWAL` VARCHAR(50) DEFAULT NULL," +
                      " `FK` VARCHAR(50) DEFAULT NULL," +
                      " `KAPASITAS` VARCHAR(50) DEFAULT NULL," +
                      " `JDL_FKZ` VARCHAR(50) DEFAULT NULL," +
                      " `PB_OTO` VARCHAR(50) DEFAULT NULL," +
                      " `KRM_EMAIL` VARCHAR(50) DEFAULT NULL," +
                      " `TP_BELI` VARCHAR(50) DEFAULT NULL," +
                      " `DISCP` VARCHAR(50) DEFAULT NULL," +
                      " `FMPNBR` VARCHAR(50) DEFAULT NULL," +
                      " `TIPE` VARCHAR(50) DEFAULT NULL," +
                      " `NILAI` VARCHAR(50) DEFAULT NULL," +
                      " `KDSUPP` VARCHAR(50) DEFAULT NULL," +
                      " `FLAG_AMBIL_SJ` VARCHAR(50) DEFAULT NULL," +
                      " `FLAGSUPP` VARCHAR(50) DEFAULT NULL," +
                      " `FLAG_BARCODE_SJ` VARCHAR(50) DEFAULT NULL," +
                      " `UPDID_CABANG` varchar(45) DEFAULT NULL," +
                      " `UPDTIME_CABANG` datetime DEFAULT NULL," +
                      " PRIMARY KEY (`KDCAB`,`SUPCO`)," +
                      " KEY `IDX_DC` (`KDCAB`)," +
                      " KEY `IDX_SUPCO` (`SUPCO`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                // ── log_master ────────────────────────────────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`log_master` (" +
                      " `JENIS` VARCHAR(25) NOT NULL," +
                      " `TGL_STATUS` DATE NOT NULL," +
                      " `TGL_PROSES` DATETIME NOT NULL," +
                      " `KODETOKO` VARCHAR(4) NOT NULL," +
                      " `IDTOKO` VARCHAR(25) NOT NULL," +
                      " `GET` VARCHAR(10) DEFAULT NULL," +
                      " `SEND` VARCHAR(10) DEFAULT NULL," +
                      " `ADDID` VARCHAR(50) DEFAULT NULL," +
                      " `ADDTIME` DATETIME DEFAULT NULL," +
                      " `UPDID` VARCHAR(50) DEFAULT NULL," +
                      " `UPDTIME` DATETIME DEFAULT NULL," +
                      " PRIMARY KEY (`JENIS`,`TGL_STATUS`,`TGL_PROSES`,`KODETOKO`)," +
                      " KEY `IDX_JENIS` (`JENIS`)," +
                      " KEY `IDX_KODETOKO` (`KODETOKO`)," +
                      " KEY `IDX_TGL_PROSES` (`TGL_PROSES`)," +
                      " KEY `IDX_IDTOKO` (`IDTOKO`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                await AlterIfIndexNotExists(conn, db_, "log_master", "JENIS", "IDX_JENIS",
                    $"ALTER TABLE `{db_}`.`log_master` ADD KEY `IDX_JENIS` (`JENIS`);");
                await AlterIfIndexNotExists(conn, db_, "log_master", "KODETOKO", "IDX_KODETOKO",
                    $"ALTER TABLE `{db_}`.`log_master` ADD KEY `IDX_KODETOKO` (`KODETOKO`);");
                await AlterIfIndexNotExists(conn, db_, "log_master", "TGL_PROSES", "IDX_TGL_PROSES",
                    $"ALTER TABLE `{db_}`.`log_master` ADD KEY `IDX_TGL_PROSES` (`TGL_PROSES`);");
                await AlterIfIndexNotExists(conn, db_, "log_master", "IDTOKO", null,
                    $"ALTER TABLE `{db_}`.`log_master` ADD KEY `IDX_IDTOKO` (`IDTOKO`);");

                // ── config ────────────────────────────────────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`config` (" +
                      " `RKEY` VARCHAR(4) NOT NULL DEFAULT ''," +
                      " `NILAI` VARCHAR(100) DEFAULT NULL," +
                      " `NILAI2` VARCHAR(100) DEFAULT NULL," +
                      " `KET` TEXT," +
                      " PRIMARY KEY (`RKEY`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                await AlterIfColumnNotExists(conn, db_, "config", "NILAI2",
                    $"ALTER TABLE `{db_}`.`config` ADD COLUMN `NILAI2` VARCHAR(100) DEFAULT NULL AFTER `NILAI`;");

                // ── master_produk_khusus ──────────────────────────────────────
                try
                {
                    sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`master_produk_khusus` (" +
                          " `KODE_MODUL` BIGINT(20) NOT NULL," +
                          " `NAMA_MODUL` VARCHAR(100) DEFAULT NULL," +
                          " `FLAG_SO` VARCHAR(1) DEFAULT NULL," +
                          " `JADWAL_HARI_SO` VARCHAR(10) DEFAULT NULL," +
                          " `PLU` VARCHAR(8) NOT NULL," +
                          " `RASIO_BELI` VARCHAR(20) DEFAULT NULL," +
                          " `SALES_GROWTH` VARCHAR(20) DEFAULT NULL," +
                          " `KELIPATAN_PRODUKSI` VARCHAR(20) DEFAULT NULL," +
                          " `TGL_STATUS` DATETIME DEFAULT NULL," +
                          " `TGL_PROSES` DATETIME DEFAULT NULL," +
                          " `ADDID` VARCHAR(50) DEFAULT NULL," +
                          " `ADDTIME` DATETIME DEFAULT NULL," +
                          " PRIMARY KEY (`KODE_MODUL`,`PLU`)" +
                          ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                    await conn.ExecuteAsync(sql);

                    await AlterIfColumnNotExists(conn, db_, "master_produk_khusus", "RASIO_BELI",
                        $"ALTER TABLE `{db_}`.`master_produk_khusus` ADD COLUMN `RASIO_BELI` VARCHAR(20) DEFAULT NULL AFTER `PLU`;");
                    await AlterIfColumnNotExists(conn, db_, "master_produk_khusus", "SALES_GROWTH",
                        $"ALTER TABLE `{db_}`.`master_produk_khusus` ADD COLUMN `SALES_GROWTH` VARCHAR(20) DEFAULT NULL AFTER `RASIO_BELI`;");
                    await AlterIfColumnNotExists(conn, db_, "master_produk_khusus", "KELIPATAN_PRODUKSI",
                        $"ALTER TABLE `{db_}`.`master_produk_khusus` ADD COLUMN `KELIPATAN_PRODUKSI` VARCHAR(20) DEFAULT NULL AFTER `SALES_GROWTH`;");
                }
                catch { /* intentionally swallowed — sama dengan VB asli */ }

                // ── master_sarana_produk_khusus ───────────────────────────────
                try
                {
                    sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`master_sarana_produk_khusus` (" +
                          " `TOK_CODE` VARCHAR(25) DEFAULT NULL," +
                          " `NAMA_MEREK` VARCHAR(100) DEFAULT NULL," +
                          " `NAMA_SARANA` VARCHAR(100) DEFAULT NULL," +
                          " `TGL_AKTIF_MEREK` VARCHAR(25) DEFAULT NULL," +
                          " `TGL_NONAKTIF_MEREK` VARCHAR(25) DEFAULT NULL," +
                          " `TGL_STATUS` DATETIME DEFAULT NULL," +
                          " `TGL_PROSES` DATETIME DEFAULT NULL," +
                          " `ADDID` VARCHAR(50) DEFAULT NULL," +
                          " `ADDTIME` DATETIME DEFAULT NULL," +
                          " KEY `IDX_TOK_CODE` (`TOK_CODE`)," +
                          " KEY `IDX_TGL_STATUS` (`TGL_STATUS`)," +
                          " KEY `IDX_TGL_PROSES` (`TGL_PROSES`)" +
                          ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                    await conn.ExecuteAsync(sql);
                }
                catch { }

                // ── master_all_tkx ────────────────────────────────────────────
                await AlterIfColumnNotExists(conn, db_, "master_all_tkx", "ALMT_TOKO",
                    $"DROP TABLE `{db_}`.`master_all_tkx`;",
                    dropIfMissing: true);
                await AlterIfColumnNotExists(conn, db_, "master_all_tkx", "MARKUP_IGR",
                    $"ALTER TABLE `{db_}`.`master_all_tkx` ADD COLUMN `MARKUP_IGR` VARCHAR(300) DEFAULT '' AFTER `FLAGTK`;");
                await AlterIfColumnNotExists(conn, db_, "master_all_tkx", "TGL_MULTIRATES",
                    $"ALTER TABLE `{db_}`.`master_all_tkx` ADD COLUMN `TGL_MULTIRATES` VARCHAR(50) DEFAULT '' AFTER `MARKUP_IGR`;");

                // ── Cek PK master_all_dcx ─────────────────────────────────────
                try
                {
                    var tblDcx = await conn.ExecuteScalarAsync<string>($"USE `{db_}`; SHOW TABLES LIKE 'master_all_dcx';");
                    if (!string.IsNullOrEmpty(tblDcx))
                    {
                        var pkCount = await conn.ExecuteScalarAsync<long>(
                            "SELECT COUNT(`COLUMN_NAME`) FROM `INFORMATION_SCHEMA`.`COLUMNS`" +
                            " WHERE `TABLE_SCHEMA` = 'trpr_cabang'" +
                            " AND `TABLE_NAME` = 'master_all_dcx'" +
                            " AND `COLUMN_KEY` = 'PRI';");
                        if (pkCount < 5)
                        {
                            await conn.ExecuteAsync($"TRUNCATE `{db_}`.`master_all_dcx`;");
                            await conn.ExecuteAsync(
                                $"ALTER TABLE `{db_}`.`master_all_dcx` DROP PRIMARY KEY;" +
                                $"ALTER TABLE `{db_}`.`master_all_dcx`" +
                                $" ADD CONSTRAINT PK_DCX PRIMARY KEY (`KODE_TOKO`,`KODE_DC`,`NAMA_DC`,`KODE_CABANG`,`NAMA_CABANG`);");
                        }
                    }
                }
                catch (Exception ex)
                {
                    _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                        Utility.TipeLog.Error);
                }

                await AlterIfColumnNotExists(conn, db_, "master_all_dcx", "FLAGPROD",
                    $"ALTER TABLE `{db_}`.`master_all_dcx` ADD COLUMN `FLAGPROD` VARCHAR(4000) DEFAULT '' AFTER `MARK_UP_IGR`;");

                // ── Bersih-bersih log_master > 7 hari ────────────────────────
                await conn.ExecuteAsync(
                    $"DELETE FROM `{db_}`.`log_master` WHERE ADDTIME < DATE_SUB(CURDATE(), INTERVAL 7 DAY);");

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> InitialTblPromoToko(string jobs, string kodeToko)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                // ── [kodetoko]_promo ──────────────────────────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `trprservice`.`{kodeToko}_promo` (" +
                      " `KODEPROMO` varchar(10) NOT NULL DEFAULT ''," +
                      " `SUBKODEPROMO` varchar(45) NOT NULL DEFAULT ''," +
                      " `TIPEPROMO` varchar(45) DEFAULT NULL," +
                      " `KODEGROUP` varchar(10) NOT NULL DEFAULT ''," +
                      " `MEKANISME` varchar(2000) DEFAULT NULL," +
                      " `CETAKSTRUK1` varchar(45) DEFAULT NULL," +
                      " `CETAKSTRUK2` varchar(45) DEFAULT NULL," +
                      " `CETAKSTRUK3` varchar(45) DEFAULT NULL," +
                      " `CETAKLAYAR1` varchar(45) DEFAULT NULL," +
                      " `CETAKLAYAR2` varchar(45) DEFAULT NULL," +
                      " `CETAKLAYAR3` varchar(45) DEFAULT NULL," +
                      " `TANGGALAWAL` varchar(45) DEFAULT NULL," +
                      " `TANGGALAKHIR` varchar(45) DEFAULT NULL," +
                      " `PERIODEJAM` varchar(45) DEFAULT NULL," +
                      " `PERIODEMINGGUAN` varchar(45) DEFAULT NULL," +
                      " `PERIODEBULANAN` varchar(45) DEFAULT NULL," +
                      " `ITEMSYARAT` longtext," +
                      " `QTYSYARATMIN` varchar(45) DEFAULT NULL," +
                      " `QTYSYARATMAX` varchar(45) DEFAULT NULL," +
                      " `QTYSYARATTAMBAH` varchar(45) DEFAULT NULL," +
                      " `RPSYARATMIN` varchar(45) DEFAULT NULL," +
                      " `RPSYARATMAX` varchar(45) DEFAULT NULL," +
                      " `RPSYARATTAMBAH` varchar(45) DEFAULT NULL," +
                      " `ITEMTARGET` longtext," +
                      " `QTYTARGET` longtext," +
                      " `QTYTARGETMAX` longtext," +
                      " `QTYTARGETTAMBAH` longtext," +
                      " `RPTARGET` longtext," +
                      " `RPTARGETMAX` longtext," +
                      " `RPTARGETTAMBAH` longtext," +
                      " `POTONGANPERSENTARGET` longtext," +
                      " `KODEBIN` longtext," +
                      " `KODEMEMBER` longtext," +
                      " `BERSYARAT` varchar(1) DEFAULT NULL," +
                      " `KODESHIFT` varchar(1) DEFAULT NULL," +
                      " `NOSTRUK` varchar(45) DEFAULT NULL," +
                      " `SYARATCAMPUR` varchar(45) DEFAULT NULL," +
                      " `TRANSAKSIMAX` longtext," +
                      " `QTYTARGETMAXPROMO` longtext," +
                      " `RPTARGETMAXPROMO` longtext," +
                      " `TARGETTDKDIJUAL` longtext," +
                      " `STICKER` varchar(1) DEFAULT NULL," +
                      " `PEMBATASAN_HADIAH` varchar(1) DEFAULT NULL," +
                      " `TENGGANGWAKTU` longtext," +
                      " `QTYMAXPERSTRUK` longtext," +
                      " `HIGHIMPACT` varchar(3) DEFAULT NULL," +
                      " `POTONGMARGIN` varchar(500) DEFAULT NULL," +
                      " `MAXRPBELANJA` varchar(45) DEFAULT NULL," +
                      " `TGL_UPDATE` varchar(45) DEFAULT NULL," +
                      " `NOURUT` bigint(20) DEFAULT NULL," +
                      " `QTY_TBS_MRH` longtext," +
                      " `KEYPROMOSI` varchar(500) DEFAULT ''," +
                      " `IKIOS` varchar(1) DEFAULT ''," +
                      " `PRIORITAS` longtext," +
                      " `ID` DECIMAL(2,0) DEFAULT '1'," +
                      " `I_STORE` varchar(1) DEFAULT ''," +
                      " `SPECIAL_PRODUCT` varchar(1) DEFAULT ''," +
                      " `PRO_SPECIAL_REQUEST` varchar(45) DEFAULT ''," +
                      " `PRO_JENIS_SPECIAL_REQ` varchar(45) DEFAULT ''," +
                      " `UPDID_CABANG` varchar(45) DEFAULT NULL," +
                      " `UPDTIME_CABANG` datetime DEFAULT NULL," +
                      " PRIMARY KEY (`KODEPROMO`,`SUBKODEPROMO`,`KODEGROUP`,`ID`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                // Alter [kodetoko]_promo
                await AlterIfColumnNotExists(conn, "trprservice", $"{kodeToko}_promo", "QTY_TBS_MRH",
                    $"ALTER TABLE `trprservice`.`{kodeToko}_promo` ADD COLUMN `QTY_TBS_MRH` longtext AFTER `NOURUT`;");
                await AlterIfColumnNotExists(conn, "trprservice", $"{kodeToko}_promo", "KEYPROMOSI",
                    $"ALTER TABLE `trprservice`.`{kodeToko}_promo` ADD COLUMN `KEYPROMOSI` varchar(500) DEFAULT '' AFTER `QTY_TBS_MRH`;");
                await AlterIfColumnNotExists(conn, "trprservice", $"{kodeToko}_promo", "IKIOS",
                    $"ALTER TABLE `trprservice`.`{kodeToko}_promo` ADD COLUMN `IKIOS` varchar(1) DEFAULT '' AFTER `KEYPROMOSI`;");
                await AlterIfColumnNotExists(conn, "trprservice", $"{kodeToko}_promo", "PRIORITAS",
                    $"ALTER TABLE `trprservice`.`{kodeToko}_promo` ADD COLUMN `PRIORITAS` longtext AFTER `IKIOS`;");
                await AlterIfColumnNotExists(conn, "trprservice", $"{kodeToko}_promo", "ID",
                    $"ALTER TABLE `trprservice`.`{kodeToko}_promo` ADD COLUMN `ID` DECIMAL(2,0) DEFAULT '1' AFTER `PRIORITAS`;");
                await AlterIfColumnNotExists(conn, "trprservice", $"{kodeToko}_promo", "I_STORE",
                    $"ALTER TABLE `trprservice`.`{kodeToko}_promo` ADD COLUMN `I_STORE` VARCHAR(1) DEFAULT '' AFTER `ID`;");
                await AlterIfColumnNotExists(conn, "trprservice", $"{kodeToko}_promo", "SPECIAL_PRODUCT",
                    $"ALTER TABLE `trprservice`.`{kodeToko}_promo` ADD COLUMN `SPECIAL_PRODUCT` VARCHAR(1) DEFAULT '' AFTER `I_STORE`;");
                await AlterIfColumnNotExists(conn, "trprservice", $"{kodeToko}_promo", "PRO_SPECIAL_REQUEST",
                    $"ALTER TABLE `trprservice`.`{kodeToko}_promo` ADD COLUMN `PRO_SPECIAL_REQUEST` VARCHAR(45) DEFAULT '' AFTER `SPECIAL_PRODUCT`;");
                await AlterIfColumnNotExists(conn, "trprservice", $"{kodeToko}_promo", "PRO_JENIS_SPECIAL_REQ",
                    $"ALTER TABLE `trprservice`.`{kodeToko}_promo` ADD COLUMN `PRO_JENIS_SPECIAL_REQ` VARCHAR(45) DEFAULT '' AFTER `PRO_SPECIAL_REQUEST`;");
                await AlterIfColumnNotExistsWithKey(conn, "trprservice", $"{kodeToko}_promo", "ID", "PRI",
                    $"ALTER TABLE `trprservice`.`{kodeToko}_promo` CHANGE `ID` `ID` DECIMAL(2,0) DEFAULT 1 NOT NULL, DROP PRIMARY KEY," +
                    $" ADD PRIMARY KEY (`KODEPROMO`, `SUBKODEPROMO`, `KODEGROUP`, `ID`);");

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> InitialTblSupplierToko(string jobs, string kodeToko)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                sql = $"CREATE TABLE IF NOT EXISTS `trprservice`.`{kodeToko}_supplier` (" +
                      " `KDCAB` VARCHAR(25) NOT NULL," +
                      " `SUPCO` VARCHAR(25) NOT NULL," +
                      " `NAMA` VARCHAR(50) DEFAULT NULL," +
                      " `ALMT1` VARCHAR(50) DEFAULT NULL," +
                      " `ALMT2` VARCHAR(50) DEFAULT NULL," +
                      " `ALMT3` VARCHAR(50) DEFAULT NULL," +
                      " `TELP1` VARCHAR(50) DEFAULT NULL," +
                      " `TELP2` VARCHAR(50) DEFAULT NULL," +
                      " `FAX` VARCHAR(50) DEFAULT NULL," +
                      " `TOP` VARCHAR(50) DEFAULT NULL," +
                      " `PKP` VARCHAR(50) DEFAULT NULL," +
                      " `NPWP` VARCHAR(50) DEFAULT NULL," +
                      " `TGL_SKP` VARCHAR(50) DEFAULT NULL," +
                      " `SKP` VARCHAR(50) DEFAULT NULL," +
                      " `DATANG` VARCHAR(50) DEFAULT NULL," +
                      " `DCONTINUE` VARCHAR(50) DEFAULT NULL," +
                      " `SS` VARCHAR(50) DEFAULT NULL," +
                      " `LT` VARCHAR(50) DEFAULT NULL," +
                      " `JADWAL` VARCHAR(50) DEFAULT NULL," +
                      " `FK` VARCHAR(50) DEFAULT NULL," +
                      " `KAPASITAS` VARCHAR(50) DEFAULT NULL," +
                      " `JDL_FKZ` VARCHAR(50) DEFAULT NULL," +
                      " `PB_OTO` VARCHAR(50) DEFAULT NULL," +
                      " `KRM_EMAIL` VARCHAR(50) DEFAULT NULL," +
                      " `TP_BELI` VARCHAR(50) DEFAULT NULL," +
                      " `DISCP` VARCHAR(50) DEFAULT NULL," +
                      " `FMPNBR` VARCHAR(50) DEFAULT NULL," +
                      " `TIPE` VARCHAR(50) DEFAULT NULL," +
                      " `NILAI` VARCHAR(50) DEFAULT NULL," +
                      " `KDSUPP` VARCHAR(50) DEFAULT NULL," +
                      " `FLAG_AMBIL_SJ` VARCHAR(50) DEFAULT NULL," +
                      " `FLAGSUPP` VARCHAR(50) DEFAULT NULL," +
                      " `FLAG_BARCODE_SJ` VARCHAR(50) DEFAULT NULL," +
                      " `UPDID_CABANG` varchar(45) DEFAULT NULL," +
                      " `UPDTIME_CABANG` datetime DEFAULT NULL," +
                      " PRIMARY KEY (`KDCAB`,`SUPCO`)," +
                      " KEY `IDX_DC` (`KDCAB`)," +
                      " KEY `IDX_SUPCO` (`SUPCO`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);
                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> InsertTrPrPromosi(string jobs, List<PROMO> objPromo)
        {
            if (objPromo.Count == 0) return true;
            try
            {
                using var conn = db.CreateConnection();
                var db_ = db.DatabaseName();

                await conn.ExecuteAsync($"TRUNCATE `{db_}`.`master_all_promo`;");

                const string insertHeader =
                    "INSERT INTO `{0}`.`master_all_promo` (" +
                    "`KODEPROMO`,`SUBKODEPROMO`,`TIPEPROMO`,`KODEGROUP`,`MEKANISME`," +
                    "`CETAKSTRUK1`,`CETAKSTRUK2`,`CETAKSTRUK3`,`CETAKLAYAR1`,`CETAKLAYAR2`," +
                    "`CETAKLAYAR3`,`TANGGALAWAL`,`TANGGALAKHIR`,`PERIODEJAM`,`PERIODEMINGGUAN`," +
                    "`PERIODEBULANAN`,`ITEMSYARAT`,`QTYSYARATMIN`,`QTYSYARATMAX`,`QTYSYARATTAMBAH`," +
                    "`RPSYARATMIN`,`RPSYARATMAX`,`RPSYARATTAMBAH`,`ITEMTARGET`,`QTYTARGET`," +
                    "`QTYTARGETMAX`,`QTYTARGETTAMBAH`,`RPTARGET`,`RPTARGETMAX`,`RPTARGETTAMBAH`," +
                    "`POTONGANPERSENTARGET`,`KODEBIN`,`KODEMEMBER`,`BERSYARAT`,`KODESHIFT`," +
                    "`NOSTRUK`,`SYARATCAMPUR`,`TRANSAKSIMAX`,`QTYTARGETMAXPROMO`,`RPTARGETMAXPROMO`," +
                    "`TARGETTDKDIJUAL`,`STICKER`,`PEMBATASAN_HADIAH`,`TENGGANGWAKTU`,`QTYMAXPERSTRUK`," +
                    "`HIGHIMPACT`,`POTONGMARGIN`,`MAXRPBELANJA`,`TGL_UPDATE`,`NOURUT`," +
                    "`QTY_TBS_MRH`,`KEYPROMOSI`,`IKIOS`,`PRIORITAS`,`ID`," +
                    "`I_STORE`,`SPECIAL_PRODUCT`,`PRO_SPECIAL_REQUEST`,`PRO_JENIS_SPECIAL_REQ`,`PRO_EXCLUDE_BIN`," +
                    "`HARIED`,`PRO_DESC_PRICE_TAG`,`PRO_JENIS_SHELFTALKER`,`UPDID_CABANG`,`UPDTIME_CABANG`) VALUES ";

                const string onDuplicateKey =
                    " ON DUPLICATE KEY UPDATE TIPEPROMO=VALUES(TIPEPROMO)," +
                    "MEKANISME=VALUES(MEKANISME),CETAKSTRUK1=VALUES(CETAKSTRUK1)," +
                    "CETAKSTRUK2=VALUES(CETAKSTRUK2),CETAKSTRUK3=VALUES(CETAKSTRUK3)," +
                    "CETAKLAYAR1=VALUES(CETAKLAYAR1),CETAKLAYAR2=VALUES(CETAKLAYAR2)," +
                    "CETAKLAYAR3=VALUES(CETAKLAYAR3),TANGGALAWAL=VALUES(TANGGALAWAL)," +
                    "TANGGALAKHIR=VALUES(TANGGALAKHIR),PERIODEJAM=VALUES(PERIODEJAM)," +
                    "PERIODEMINGGUAN=VALUES(PERIODEMINGGUAN),PERIODEBULANAN=VALUES(PERIODEBULANAN)," +
                    "ITEMSYARAT=VALUES(ITEMSYARAT),QTYSYARATMIN=VALUES(QTYSYARATMIN)," +
                    "QTYSYARATMAX=VALUES(QTYSYARATMAX),QTYSYARATTAMBAH=VALUES(QTYSYARATTAMBAH)," +
                    "RPSYARATMIN=VALUES(RPSYARATMIN),RPSYARATMAX=VALUES(RPSYARATMAX)," +
                    "RPSYARATTAMBAH=VALUES(RPSYARATTAMBAH),ITEMTARGET=VALUES(ITEMTARGET)," +
                    "QTYTARGET=VALUES(QTYTARGET),QTYTARGETMAX=VALUES(QTYTARGETMAX)," +
                    "QTYTARGETTAMBAH=VALUES(QTYTARGETTAMBAH),RPTARGET=VALUES(RPTARGET)," +
                    "RPTARGETMAX=VALUES(RPTARGETMAX),RPTARGETTAMBAH=VALUES(RPTARGETTAMBAH)," +
                    "POTONGANPERSENTARGET=VALUES(POTONGANPERSENTARGET),KODEBIN=VALUES(KODEBIN)," +
                    "KODEMEMBER=VALUES(KODEMEMBER),BERSYARAT=VALUES(BERSYARAT)," +
                    "KODESHIFT=VALUES(KODESHIFT),NOSTRUK=VALUES(NOSTRUK)," +
                    "SYARATCAMPUR=VALUES(SYARATCAMPUR),TRANSAKSIMAX=VALUES(TRANSAKSIMAX)," +
                    "QTYTARGETMAXPROMO=VALUES(QTYTARGETMAXPROMO),RPTARGETMAXPROMO=VALUES(RPTARGETMAXPROMO)," +
                    "TARGETTDKDIJUAL=VALUES(TARGETTDKDIJUAL),STICKER=VALUES(STICKER)," +
                    "PEMBATASAN_HADIAH=VALUES(PEMBATASAN_HADIAH),TENGGANGWAKTU=VALUES(TENGGANGWAKTU)," +
                    "QTYMAXPERSTRUK=VALUES(QTYMAXPERSTRUK),HIGHIMPACT=VALUES(HIGHIMPACT)," +
                    "POTONGMARGIN=VALUES(POTONGMARGIN),MAXRPBELANJA=VALUES(MAXRPBELANJA)," +
                    "TGL_UPDATE=VALUES(TGL_UPDATE),NOURUT=VALUES(NOURUT),QTY_TBS_MRH=VALUES(QTY_TBS_MRH)," +
                    "KEYPROMOSI=VALUES(KEYPROMOSI),IKIOS=VALUES(IKIOS),PRIORITAS=VALUES(PRIORITAS)," +
                    "ID=VALUES(ID),I_STORE=VALUES(I_STORE),SPECIAL_PRODUCT=VALUES(SPECIAL_PRODUCT)," +
                    "PRO_SPECIAL_REQUEST=VALUES(PRO_SPECIAL_REQUEST),PRO_JENIS_SPECIAL_REQ=VALUES(PRO_JENIS_SPECIAL_REQ)," +
                    "PRO_EXCLUDE_BIN=VALUES(PRO_EXCLUDE_BIN),HARIED=VALUES(HARIED)," +
                    "PRO_DESC_PRICE_TAG=VALUES(PRO_DESC_PRICE_TAG),PRO_JENIS_SHELFTALKER=VALUES(PRO_JENIS_SHELFTALKER)," +
                    "UPDID_CABANG=VALUES(UPDID_CABANG),UPDTIME_CABANG=VALUES(UPDTIME_CABANG);";

                await ExecuteBatchInsert(conn, objPromo, batchSize: 20,
                    header: string.Format(insertHeader, db_),
                    onDuplicate: onDuplicateKey,
                    rowBuilder: pro =>
                    {
                        string V(string? val) => (val ?? "").Replace("'", "''").Replace("\\", "\\\\");
                        string Vs(string? val) => (val ?? "").Replace("'", "").Replace("\\", "\\\\");

                        var id = Vs(pro.ID).Length > 0 ? Vs(pro.ID) : "0";
                        var iStore = Vs(pro.I_STORE);
                        var specialProduct = Vs(pro.SPECIAL_PRODUCT);
                        var proSpecialReq = Vs(pro.PRO_SPECIAL_REQUEST);
                        var proJenisSpecReq = Vs(pro.PRO_JENIS_SPECIAL_REQ);
                        var proExcludeBin = Vs(pro.PRO_EXCLUDE_BIN);
                        var haried = Vs(pro.HARIED);
                        var proDescPriceTag = Vs(pro.PRO_DESC_PRICE_TAG);
                        var proJenisShelft = Vs(pro.PRO_JENIS_SHELFTALKER);

                        return $"('{V(pro.KODEPROMO)}','{V(pro.SUBKODEPROMO)}','{V(pro.TIPEPROMO)}'," +
                               $"'{V(pro.KODEGROUP)}','{V(pro.MEKANISME)}','{V(pro.CETAKSTRUK1)}'," +
                               $"'{V(pro.CETAKSTRUK2)}','{V(pro.CETAKSTRUK3)}','{V(pro.CETAKLAYAR1)}'," +
                               $"'{V(pro.CETAKLAYAR2)}','{V(pro.CETAKLAYAR3)}','{V(pro.TANGGALAWAL)}'," +
                               $"'{V(pro.TANGGALAKHIR)}','{V(pro.PERIODEJAM)}','{V(pro.PERIODEMINGGUAN)}'," +
                               $"'{V(pro.PERIODEBULANAN)}','{V(pro.ITEMSYARAT)}','{V(pro.QTYSYARATMIN)}'," +
                               $"'{V(pro.QTYSYARATMAX)}','{V(pro.QTYSYARATTAMBAH)}','{V(pro.RPSYARATMIN)}'," +
                               $"'{V(pro.RPSYARATMAX)}','{V(pro.RPSYARATTAMBAH)}','{V(pro.ITEMTARGET)}'," +
                               $"'{V(pro.QTYTARGET)}','{V(pro.QTYTARGETMAX)}','{V(pro.QTYTARGETTAMBAH)}'," +
                               $"'{V(pro.RPTARGET)}','{V(pro.RPTARGETMAX)}','{V(pro.RPTARGETTAMBAH)}'," +
                               $"'{V(pro.POTONGANPERSENTARGET)}','{V(pro.KODEBIN)}','{V(pro.KODEMEMBER)}'," +
                               $"'{V(pro.BERSYARAT)}','{V(pro.KODESHIFT)}','{V(pro.NOSTRUK)}'," +
                               $"'{V(pro.SYARATCAMPUR)}','{V(pro.TRANSAKSIMAX)}','{V(pro.QTYTARGETMAXPROMO)}'," +
                               $"'{V(pro.RPTARGETMAXPROMO)}','{V(pro.TARGETTDKDIJUAL)}','{V(pro.STICKER)}'," +
                               $"'{V(pro.PEMBATASAN_HADIAH)}','{V(pro.TENGGANGWAKTU)}','{V(pro.QTYMAXPERSTRUK)}'," +
                               $"'{V(pro.FLHIMPACT)}','{V(pro.POTONGMARGIN)}','{V(pro.MAXRPBELANJA)}'," +
                               $"'{V(pro.TGL_UPDATE)}','{pro.NOURUT}','{V(pro.QTY_TBS_MRH)}'," +
                               $"'{V(pro.KEYPROMOSI)}','{V(pro.IKIOS)}','{V(pro.PRIORITAS)}'," +
                               $"{id},'{iStore}','{specialProduct}'," +
                               $"'{proSpecialReq}','{proJenisSpecReq}','{proExcludeBin}'," +
                               $"'{haried}','{proDescPriceTag}','{proJenisShelft}'," +
                               $"USER(),NOW())";
                    });

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> InsertMasterSupplier(string jobs, List<SUPPLIER> objSupplier)
        {
            if (objSupplier.Count == 0) return true;
            string currentSupco = "";
            try
            {
                using var conn = db.CreateConnection();
                var db_ = db.DatabaseName();

                const string insertHeader =
                    "INSERT INTO `{0}`.`master_all_supplier` (" +
                    "`KDCAB`,`SUPCO`,`NAMA`,`ALMT1`,`ALMT2`,`ALMT3`," +
                    "`TELP1`,`TELP2`,`FAX`,`TOP`,`PKP`,`NPWP`," +
                    "`TGL_SKP`,`SKP`,`DATANG`,`DCONTINUE`,`SS`,`LT`," +
                    "`JADWAL`,`FK`,`KAPASITAS`,`JDL_FKZ`,`PB_OTO`,`KRM_EMAIL`," +
                    "`TP_BELI`,`DISCP`,`FMPNBR`,`TIPE`,`NILAI`,`KDSUPP`," +
                    "`FLAG_AMBIL_SJ`,`FLAGSUPP`,`FLAG_BARCODE_SJ`," +
                    "`UPDID_CABANG`,`UPDTIME_CABANG`) VALUES ";

                const string onDuplicateKey =
                    " ON DUPLICATE KEY UPDATE NAMA=VALUES(NAMA)," +
                    "KDCAB=VALUES(KDCAB),SUPCO=VALUES(SUPCO)," +
                    "ALMT1=VALUES(ALMT1),ALMT2=VALUES(ALMT2),ALMT3=VALUES(ALMT3)," +
                    "TELP1=VALUES(TELP1),TELP2=VALUES(TELP2),FAX=VALUES(FAX)," +
                    "TOP=VALUES(TOP),PKP=VALUES(PKP),NPWP=VALUES(NPWP)," +
                    "TGL_SKP=VALUES(TGL_SKP),SKP=VALUES(SKP),DATANG=VALUES(DATANG)," +
                    "DCONTINUE=VALUES(DCONTINUE),SS=VALUES(SS),LT=VALUES(LT)," +
                    "JADWAL=VALUES(JADWAL),FK=VALUES(FK),KAPASITAS=VALUES(KAPASITAS)," +
                    "JDL_FKZ=VALUES(JDL_FKZ),PB_OTO=VALUES(PB_OTO),KRM_EMAIL=VALUES(KRM_EMAIL)," +
                    "TP_BELI=VALUES(TP_BELI),DISCP=VALUES(DISCP),FMPNBR=VALUES(FMPNBR)," +
                    "TIPE=VALUES(TIPE),NILAI=VALUES(NILAI),KDSUPP=VALUES(KDSUPP)," +
                    "FLAG_AMBIL_SJ=VALUES(FLAG_AMBIL_SJ),FLAGSUPP=VALUES(FLAGSUPP)," +
                    "FLAG_BARCODE_SJ=VALUES(FLAG_BARCODE_SJ)," +
                    "UPDID_CABANG=VALUES(UPDID_CABANG),UPDTIME_CABANG=VALUES(UPDTIME_CABANG);";

                await ExecuteBatchInsert(conn, objSupplier, batchSize: 20,
                    header: string.Format(insertHeader, db_),
                    onDuplicate: onDuplicateKey,
                    rowBuilder: supp =>
                    {
                        currentSupco = supp.SUPCO ?? "";
                        string V(string? val) => (val ?? "").Replace("'", "''").Replace("\\", "\\\\");

                        return $"('{V(supp.KDCAB)}','{V(supp.SUPCO)}','{V(supp.NAMA)}'," +
                               $"'{V(supp.ALMT1)}','{V(supp.ALMT2)}','{V(supp.ALMT3)}'," +
                               $"'{V(supp.TELP1)}','{V(supp.TELP2)}','{V(supp.FAX)}'," +
                               $"'{V(supp.TOP)}','{V(supp.PKP)}','{V(supp.NPWP)}'," +
                               $"'{V(supp.TGL_SKP)}','{V(supp.SKP)}','{V(supp.DATANG)}'," +
                               $"'{V(supp.DCONTINUE)}','{V(supp.SS)}','{V(supp.LT)}'," +
                               $"'{V(supp.JADWAL)}','{V(supp.FK)}','{V(supp.KAPASITAS)}'," +
                               $"'{V(supp.JDL_FKZ)}','{V(supp.PB_OTO)}','{V(supp.KRM_EMAIL)}'," +
                               $"'{V(supp.TP_BELI)}','{V(supp.DISCP)}','{V(supp.FMPNBR)}'," +
                               $"'{V(supp.TIPE)}','{V(supp.NILAI)}','{V(supp.KDSUPP)}'," +
                               $"'{V(supp.FLAG_AMBIL_SJ)}','{V(supp.FLAGSUPP)}','{V(supp.FLAG_BARCODE_SJ)}'," +
                               $"USER(),NOW())";
                    });

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSUPCO: " + currentSupco,
                    Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> InsertPromosiToko(string jobs, string kodeToko, List<dynamic> dtPromosi)
        {
            if (dtPromosi.Count == 0) return true;
            try
            {
                using var conn = db.CreateConnection();

                // ── DELETE bersih-bersih sebelum INSERT ───────────────────────
                try
                {
                    var listKdPromo = string.Join(",",
                        dtPromosi.Select(r => $"'{((string)r.KODEPROMO.ToString()).Replace("'", "''").Replace("\\", "\\\\")}'"));

                    await conn.ExecuteAsync(
                        $"DELETE FROM `trprservice`.`{kodeToko}_promo`" +
                        $" WHERE `UPDTIME_CABANG` < DATE_SUB(CURDATE(), INTERVAL 30 DAY)" +
                        $" OR `KODEPROMO` IN ({listKdPromo});");
                }
                catch (Exception ex)
                {
                    _objUtil.Tracelog(jobs,
                        $"Gagal Bersih2 Data Table `{kodeToko}_promo`.\r\n{ex.Message}",
                        Utility.TipeLog.Error);
                }

                // ── Batch INSERT ──────────────────────────────────────────────
                var header =
                    $"INSERT INTO `trprservice`.`{kodeToko}_promo` (" +
                    "`KODEPROMO`,`SUBKODEPROMO`,`TIPEPROMO`,`KODEGROUP`,`MEKANISME`," +
                    "`CETAKSTRUK1`,`CETAKSTRUK2`,`CETAKSTRUK3`,`CETAKLAYAR1`,`CETAKLAYAR2`," +
                    "`CETAKLAYAR3`,`TANGGALAWAL`,`TANGGALAKHIR`,`PERIODEJAM`,`PERIODEMINGGUAN`," +
                    "`PERIODEBULANAN`,`ITEMSYARAT`,`QTYSYARATMIN`,`QTYSYARATMAX`,`QTYSYARATTAMBAH`," +
                    "`RPSYARATMIN`,`RPSYARATMAX`,`RPSYARATTAMBAH`,`ITEMTARGET`,`QTYTARGET`," +
                    "`QTYTARGETMAX`,`QTYTARGETTAMBAH`,`RPTARGET`,`RPTARGETMAX`,`RPTARGETTAMBAH`," +
                    "`POTONGANPERSENTARGET`,`KODEBIN`,`KODEMEMBER`,`BERSYARAT`,`KODESHIFT`," +
                    "`NOSTRUK`,`SYARATCAMPUR`,`TRANSAKSIMAX`,`QTYTARGETMAXPROMO`,`RPTARGETMAXPROMO`," +
                    "`TARGETTDKDIJUAL`,`STICKER`,`PEMBATASAN_HADIAH`,`TENGGANGWAKTU`,`QTYMAXPERSTRUK`," +
                    "`HIGHIMPACT`,`POTONGMARGIN`,`MAXRPBELANJA`,`TGL_UPDATE`,`NOURUT`," +
                    "`QTY_TBS_MRH`,`KEYPROMOSI`,`IKIOS`,`PRIORITAS`,`ID`," +
                    "`I_STORE`,`SPECIAL_PRODUCT`,`PRO_SPECIAL_REQUEST`,`PRO_JENIS_SPECIAL_REQ`," +
                    "`UPDID_CABANG`,`UPDTIME_CABANG`) VALUES ";

                const string onDuplicateKey =
                    " ON DUPLICATE KEY UPDATE TIPEPROMO=VALUES(TIPEPROMO)," +
                    "MEKANISME=VALUES(MEKANISME),CETAKSTRUK1=VALUES(CETAKSTRUK1)," +
                    "CETAKSTRUK2=VALUES(CETAKSTRUK2),CETAKSTRUK3=VALUES(CETAKSTRUK3)," +
                    "CETAKLAYAR1=VALUES(CETAKLAYAR1),CETAKLAYAR2=VALUES(CETAKLAYAR2)," +
                    "CETAKLAYAR3=VALUES(CETAKLAYAR3),TANGGALAWAL=VALUES(TANGGALAWAL)," +
                    "TANGGALAKHIR=VALUES(TANGGALAKHIR),PERIODEJAM=VALUES(PERIODEJAM)," +
                    "PERIODEMINGGUAN=VALUES(PERIODEMINGGUAN),PERIODEBULANAN=VALUES(PERIODEBULANAN)," +
                    "ITEMSYARAT=VALUES(ITEMSYARAT),QTYSYARATMIN=VALUES(QTYSYARATMIN)," +
                    "QTYSYARATMAX=VALUES(QTYSYARATMAX),QTYSYARATTAMBAH=VALUES(QTYSYARATTAMBAH)," +
                    "RPSYARATMIN=VALUES(RPSYARATMIN),RPSYARATMAX=VALUES(RPSYARATMAX)," +
                    "RPSYARATTAMBAH=VALUES(RPSYARATTAMBAH),ITEMTARGET=VALUES(ITEMTARGET)," +
                    "QTYTARGET=VALUES(QTYTARGET),QTYTARGETMAX=VALUES(QTYTARGETMAX)," +
                    "QTYTARGETTAMBAH=VALUES(QTYTARGETTAMBAH),RPTARGET=VALUES(RPTARGET)," +
                    "RPTARGETMAX=VALUES(RPTARGETMAX),RPTARGETTAMBAH=VALUES(RPTARGETTAMBAH)," +
                    "POTONGANPERSENTARGET=VALUES(POTONGANPERSENTARGET),KODEBIN=VALUES(KODEBIN)," +
                    "KODEMEMBER=VALUES(KODEMEMBER),BERSYARAT=VALUES(BERSYARAT)," +
                    "KODESHIFT=VALUES(KODESHIFT),NOSTRUK=VALUES(NOSTRUK)," +
                    "SYARATCAMPUR=VALUES(SYARATCAMPUR),TRANSAKSIMAX=VALUES(TRANSAKSIMAX)," +
                    "QTYTARGETMAXPROMO=VALUES(QTYTARGETMAXPROMO),RPTARGETMAXPROMO=VALUES(RPTARGETMAXPROMO)," +
                    "TARGETTDKDIJUAL=VALUES(TARGETTDKDIJUAL),STICKER=VALUES(STICKER)," +
                    "PEMBATASAN_HADIAH=VALUES(PEMBATASAN_HADIAH),TENGGANGWAKTU=VALUES(TENGGANGWAKTU)," +
                    "QTYMAXPERSTRUK=VALUES(QTYMAXPERSTRUK),HIGHIMPACT=VALUES(HIGHIMPACT)," +
                    "POTONGMARGIN=VALUES(POTONGMARGIN),MAXRPBELANJA=VALUES(MAXRPBELANJA)," +
                    "TGL_UPDATE=VALUES(TGL_UPDATE),NOURUT=VALUES(NOURUT),QTY_TBS_MRH=VALUES(QTY_TBS_MRH)," +
                    "KEYPROMOSI=VALUES(KEYPROMOSI),IKIOS=VALUES(IKIOS),PRIORITAS=VALUES(PRIORITAS)," +
                    "ID=VALUES(ID),I_STORE=VALUES(I_STORE),SPECIAL_PRODUCT=VALUES(SPECIAL_PRODUCT)," +
                    "PRO_SPECIAL_REQUEST=VALUES(PRO_SPECIAL_REQUEST),PRO_JENIS_SPECIAL_REQ=VALUES(PRO_JENIS_SPECIAL_REQ)," +
                    "UPDID_CABANG=VALUES(UPDID_CABANG),UPDTIME_CABANG=VALUES(UPDTIME_CABANG);";

                await ExecuteBatchInsert(conn, dtPromosi, batchSize: 25,
                    header: header,
                    onDuplicate: onDuplicateKey,
                    rowBuilder: pro =>
                    {
                        string V(string? val) => (val ?? "").Replace("'", "''").Replace("\\", "\\\\");
                        string Col(string key) => V(((IDictionary<string, object>)pro)[key]?.ToString());

                        return $"('{Col("KODEPROMO")}','{Col("SUBKODEPROMO")}','{Col("TIPEPROMO")}'," +
                               $"'{Col("KODEGROUP")}','{Col("MEKANISME")}','{Col("CETAKSTRUK1")}'," +
                               $"'{Col("CETAKSTRUK2")}','{Col("CETAKSTRUK3")}','{Col("CETAKLAYAR1")}'," +
                               $"'{Col("CETAKLAYAR2")}','{Col("CETAKLAYAR3")}','{Col("TANGGALAWAL")}'," +
                               $"'{Col("TANGGALAKHIR")}','{Col("PERIODEJAM")}','{Col("PERIODEMINGGUAN")}'," +
                               $"'{Col("PERIODEBULANAN")}','{Col("ITEMSYARAT")}','{Col("QTYSYARATMIN")}'," +
                               $"'{Col("QTYSYARATMAX")}','{Col("QTYSYARATTAMBAH")}','{Col("RPSYARATMIN")}'," +
                               $"'{Col("RPSYARATMAX")}','{Col("RPSYARATTAMBAH")}','{Col("ITEMTARGET")}'," +
                               $"'{Col("QTYTARGET")}','{Col("QTYTARGETMAX")}','{Col("QTYTARGETTAMBAH")}'," +
                               $"'{Col("RPTARGET")}','{Col("RPTARGETMAX")}','{Col("RPTARGETTAMBAH")}'," +
                               $"'{Col("POTONGANPERSENTARGET")}','{Col("KODEBIN")}','{Col("KODEMEMBER")}'," +
                               $"'{Col("BERSYARAT")}','{Col("KODESHIFT")}','{Col("NOSTRUK")}'," +
                               $"'{Col("SYARATCAMPUR")}','{Col("TRANSAKSIMAX")}','{Col("QTYTARGETMAXPROMO")}'," +
                               $"'{Col("RPTARGETMAXPROMO")}','{Col("TARGETTDKDIJUAL")}','{Col("STICKER")}'," +
                               $"'{Col("PEMBATASAN_HADIAH")}','{Col("TENGGANGWAKTU")}','{Col("QTYMAXPERSTRUK")}'," +
                               $"'{Col("HIGHIMPACT")}','{Col("POTONGMARGIN")}','{Col("MAXRPBELANJA")}'," +
                               $"'{Col("TGL_UPDATE")}','{Col("NOURUT")}','{Col("QTY_TBS_MRH")}'," +
                               $"'{Col("KEYPROMOSI")}','{Col("IKIOS")}','{Col("PRIORITAS")}'," +
                               $"{Col("ID")},'{Col("I_STORE")}','{Col("SPECIAL_PRODUCT")}'," +
                               $"'{Col("PRO_SPECIAL_REQUEST")}','{Col("PRO_JENIS_SPECIAL_REQ")}'," +
                               $"USER(),NOW())";
                    });

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> InsertSupplierToko(string jobs, string kodeToko, List<dynamic> dtSupplier)
        {
            if (dtSupplier.Count == 0) return true;
            string currentSupco = "";
            try
            {
                using var conn = db.CreateConnection();

                var header =
                    $"INSERT INTO `trprservice`.`{kodeToko}_supplier` (" +
                    "`KDCAB`,`SUPCO`,`NAMA`,`ALMT1`,`ALMT2`,`ALMT3`," +
                    "`TELP1`,`TELP2`,`FAX`,`TOP`,`PKP`,`NPWP`," +
                    "`TGL_SKP`,`SKP`,`DATANG`,`DCONTINUE`,`SS`,`LT`," +
                    "`JADWAL`,`FK`,`KAPASITAS`,`JDL_FKZ`,`PB_OTO`,`KRM_EMAIL`," +
                    "`TP_BELI`,`DISCP`,`FMPNBR`,`TIPE`,`NILAI`,`KDSUPP`," +
                    "`FLAG_AMBIL_SJ`,`FLAGSUPP`,`FLAG_BARCODE_SJ`," +
                    "`UPDID_CABANG`,`UPDTIME_CABANG`) VALUES ";

                const string onDuplicateKey =
                    " ON DUPLICATE KEY UPDATE NAMA=VALUES(NAMA)," +
                    "KDCAB=VALUES(KDCAB),SUPCO=VALUES(SUPCO)," +
                    "ALMT1=VALUES(ALMT1),ALMT2=VALUES(ALMT2),ALMT3=VALUES(ALMT3)," +
                    "TELP1=VALUES(TELP1),TELP2=VALUES(TELP2),FAX=VALUES(FAX)," +
                    "TOP=VALUES(TOP),PKP=VALUES(PKP),NPWP=VALUES(NPWP)," +
                    "TGL_SKP=VALUES(TGL_SKP),SKP=VALUES(SKP),DATANG=VALUES(DATANG)," +
                    "DCONTINUE=VALUES(DCONTINUE),SS=VALUES(SS),LT=VALUES(LT)," +
                    "JADWAL=VALUES(JADWAL),FK=VALUES(FK),KAPASITAS=VALUES(KAPASITAS)," +
                    "JDL_FKZ=VALUES(JDL_FKZ),PB_OTO=VALUES(PB_OTO),KRM_EMAIL=VALUES(KRM_EMAIL)," +
                    "TP_BELI=VALUES(TP_BELI),DISCP=VALUES(DISCP),FMPNBR=VALUES(FMPNBR)," +
                    "TIPE=VALUES(TIPE),NILAI=VALUES(NILAI),KDSUPP=VALUES(KDSUPP)," +
                    "FLAG_AMBIL_SJ=VALUES(FLAG_AMBIL_SJ),FLAGSUPP=VALUES(FLAGSUPP)," +
                    "FLAG_BARCODE_SJ=VALUES(FLAG_BARCODE_SJ)," +
                    "UPDID_CABANG=VALUES(UPDID_CABANG),UPDTIME_CABANG=VALUES(UPDTIME_CABANG);";

                await ExecuteBatchInsert(conn, dtSupplier, batchSize: 20,
                    header: header,
                    onDuplicate: onDuplicateKey,
                    rowBuilder: dr =>
                    {
                        string V(string? val) => (val ?? "").Replace("'", "''").Replace("\\", "\\\\");
                        string Col(string key) => V(((IDictionary<string, object>)dr)[key]?.ToString());
                        currentSupco = Col("SUPCO");

                        return $"('{Col("KDCAB")}','{Col("SUPCO")}','{Col("NAMA")}'," +
                               $"'{Col("ALMT1")}','{Col("ALMT2")}','{Col("ALMT3")}'," +
                               $"'{Col("TELP1")}','{Col("TELP2")}','{Col("FAX")}'," +
                               $"'{Col("TOP")}','{Col("PKP")}','{Col("NPWP")}'," +
                               $"'{Col("TGL_SKP")}','{Col("SKP")}','{Col("DATANG")}'," +
                               $"'{Col("DCONTINUE")}','{Col("SS")}','{Col("LT")}'," +
                               $"'{Col("JADWAL")}','{Col("FK")}','{Col("KAPASITAS")}'," +
                               $"'{Col("JDL_FKZ")}','{Col("PB_OTO")}','{Col("KRM_EMAIL")}'," +
                               $"'{Col("TP_BELI")}','{Col("DISCP")}','{Col("FMPNBR")}'," +
                               $"'{Col("TIPE")}','{Col("NILAI")}','{Col("KDSUPP")}'," +
                               $"'{Col("FLAG_AMBIL_SJ")}','{Col("FLAGSUPP")}','{Col("FLAG_BARCODE_SJ")}'," +
                               $"USER(),NOW())";
                    });

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSUPCO: " + currentSupco,
                    Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> InsertPartisipanPromosi(string jobs, List<PARTISIPAN_PROMO> objPartisipan, string tglStatus, string tglProses)
        {
            if (objPartisipan.Count == 0) return true;
            try
            {
                using var conn = db.CreateConnection();
                var db_ = db.DatabaseName();

                await conn.ExecuteAsync($"TRUNCATE `{db_}`.`partisipan_promo`;");

                const string header =
                    "INSERT IGNORE INTO `{0}`.`partisipan_promo` (" +
                    "`KD_DC`,`KD_TOKO`,`KD_PROMO`," +
                    "`NO_URUT`,`TGL_UPDATE`,`TGL_STATUS`," +
                    "`TGL_PROSES`,`UPDID_CABANG`,`UPDTIME_CABANG`) VALUES ";

                // INSERT IGNORE tidak pakai ON DUPLICATE KEY — suffix hanya ";"
                await ExecuteBatchInsert(conn, objPartisipan, batchSize: 1000,
                    header: string.Format(header, db_),
                    onDuplicate: ";",
                    rowBuilder: par =>
                        $"('{par.KD_DC}','{par.KD_TOKO}','{par.KD_PROMO}'," +
                        $"'{par.NO_URUT}','{par.TGL_UPDATE}','{tglStatus}'," +
                        $"'{tglProses}',USER(),NOW())");

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> InsertPartisipanPajak(string jobs, List<PAJAK> objPajak, string tglStatus, string tglProses)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                if (objPajak.Count > 0)
                {
                    // TRUNCATE
                    sql = $"TRUNCATE `{db.DatabaseName()}`.`partisipan_pajak`;";
                    await conn.ExecuteAsync(sql);

                    int batchSize = 25;

                    for (int i = 0; i < objPajak.Count; i += batchSize)
                    {
                        var batch = objPajak.Skip(i).Take(batchSize);

                        var values = string.Join(",", batch.Select(p =>
                            $"('{p.KD_DC}','{p.TOK_CODE}','{p.TOK_SKP}'," +
                            $"'{p.TOK_NAMA_FRC}','{p.TOK_NPWP}','{p.START_TGL_BERLAKU_SE}'," +
                            $"'{p.END_TGL_BERLAKU_SE}','{p.FLAG_PKP}','{tglStatus}'," +
                            $"'{tglProses}',USER(),NOW())"
                        ));

                        sql = $@"
                                    INSERT IGNORE INTO `{db.DatabaseName()}`.`partisipan_pajak`
                                    (`KD_DC`,`TOK_CODE`,`TOK_SKP`,
                                     `TOK_NAMA_FRC`,`TOK_NPWP`,`START_TGL_BERLAKU_SE`,
                                     `END_TGL_BERLAKU_SE`,`FLAG_PKP`,`TGL_STATUS`,
                                     `TGL_PROSES`,`UPDID_CABANG`,`UPDTIME_CABANG`)
                                    VALUES {values};";

                        await conn.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertPartisipanSupplier(string jobs, List<PARTISIPAN_SUPPLIER> objPartisipan, string tglStatus, string tglProses)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                if (objPartisipan.Count > 0)
                {
                    sql = $"TRUNCATE `{db.DatabaseName()}`.`partisipan_supplier`;";
                    await conn.ExecuteAsync(sql);

                    int batchSize = 26;

                    for (int i = 0; i < objPartisipan.Count; i += batchSize)
                    {
                        var batch = objPartisipan.Skip(i).Take(batchSize);

                        var values = string.Join(",", batch.Select(p =>
                            $"('{p.KDCAB}','{p.KDTK}','{p.SUPCO}'," +
                            $"{p.LT},'{p.JADWAL}','{p.DATANG}'," +
                            $"{p.KAPASITAS},'{p.FAX}'," +
                            $"'{tglStatus}','{tglProses}',NOW())"
                        ));

                        sql = $@"
                                INSERT IGNORE INTO `{db.DatabaseName()}`.`partisipan_supplier`
                                (`KDCAB`,`KDTK`,`SUPCO`,
                                 `LT`,`JADWAL`,`DATANG`,
                                 `KAPASITAS`,`FAX`,
                                 `TGL_STATUS`,`TGL_PROSES`,`UPDTIME_CABANG`)
                                VALUES {values};";

                        var count = await conn.ExecuteAsync(sql);

                        _objUtil.Tracelog(jobs,
                            sql + "\r\nCount: " + count,
                            Utility.TipeLog.Debug);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql, Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertMasterProdukKhusus(string jobs, List<PRODUK_KHUSUS> objProdKhusus, DateTime tglStatus, DateTime tglProses)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                sql = $"TRUNCATE `{db.DatabaseName()}`.`master_produk_khusus`;";
                await conn.ExecuteAsync(sql);

                if (objProdKhusus.Count > 0)
                {
                    int batchSize = 50;

                    for (int i = 0; i < objProdKhusus.Count; i += batchSize)
                    {
                        var batch = objProdKhusus.Skip(i).Take(batchSize);

                        var values = string.Join(",", batch.Select(p =>
                            $"('{Escape(p.KODE_MODUL)}','{Escape(p.NAMA_MODUL)}','{Escape(p.FLAG_SO)}'," +
                            $"'{Escape(p.JADWAL_HARI_SO)}','{Escape(p.PLU)}'," +
                            $"'{Escape(p.RASIO_BELI)}','{Escape(p.SALES_GROWTH)}','{Escape(p.KELIPATAN_PRODUKSI)}'," +
                            $"'{tglStatus:yyyy-MM-dd HH:mm:ss}','{tglProses:yyyy-MM-dd HH:mm:ss}',USER(),NOW())"
                        ));

                        sql = $@"
                                INSERT INTO `{db.DatabaseName()}`.`master_produk_khusus`
                                (`KODE_MODUL`,`NAMA_MODUL`,`FLAG_SO`,
                                 `JADWAL_HARI_SO`,`PLU`,
                                 `RASIO_BELI`,`SALES_GROWTH`,`KELIPATAN_PRODUKSI`,
                                 `TGL_STATUS`,`TGL_PROSES`,`ADDID`,`ADDTIME`)
                                VALUES {values};";

                        await conn.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql, Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertMasterSaranaProdukKhusus(string jobs, List<SARANA_PRODUK_KHUSUS> objSaranaProdKhusus, DateTime tglStatus, DateTime tglProses)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                // TRUNCATE
                sql = $"TRUNCATE `{db.DatabaseName()}`.`master_sarana_produk_khusus`;";
                await conn.ExecuteAsync(sql);

                if (objSaranaProdKhusus.Count > 0)
                {
                    int batchSize = 100;

                    for (int i = 0; i < objSaranaProdKhusus.Count; i += batchSize)
                    {
                        var batch = objSaranaProdKhusus.Skip(i).Take(batchSize);

                        var values = string.Join(",", batch.Select(p =>
                            $"('{Escape(p.TOK_CODE)}','{Escape(p.NAMA_MEREK)}','{Escape(p.NAMA_SARANA)}'," +
                            $"'{Escape(p.TGL_AKTIF_MEREK)}','{Escape(p.TGL_NONAKTIF_MEREK)}'," +
                            $"'{tglStatus:yyyy-MM-dd HH:mm:ss}','{tglProses:yyyy-MM-dd HH:mm:ss}',USER(),NOW())"
                        ));

                        sql = $@"
                                INSERT INTO `{db.DatabaseName()}`.`master_sarana_produk_khusus`
                                (`TOK_CODE`,`NAMA_MEREK`,`NAMA_SARANA`,
                                 `TGL_AKTIF_MEREK`,`TGL_NONAKTIF_MEREK`,`TGL_STATUS`,
                                 `TGL_PROSES`,`ADDID`,`ADDTIME`)
                                VALUES {values};";

                        await conn.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertMasterTTT(string jobs, List<TTT> objTTT, DateTime tglStatus, DateTime tglProses)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                // CREATE TABLE
                sql = $@"
                        CREATE TABLE IF NOT EXISTS `{db.DatabaseName()}`.`master_ttt` (
                         `TAT_ID` VARCHAR(50) NOT NULL DEFAULT '',
                         `KD_TOKO` VARCHAR(5) NOT NULL DEFAULT '',
                         `KD_TOKO_PENERIMA` VARCHAR(5) NOT NULL DEFAULT '',
                         `TGL_STATUS` DATETIME DEFAULT NULL,
                         `TGL_PROSES` DATETIME DEFAULT NULL,
                         `UPDID_CABANG` VARCHAR(50) DEFAULT NULL,
                         `UPDTIME_CABANG` DATETIME DEFAULT NULL,
                         PRIMARY KEY (`TAT_ID`,`KD_TOKO`,`KD_TOKO_PENERIMA`)
                        ) ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                if (objTTT.Count > 0)
                {
                    sql = $"TRUNCATE `{db.DatabaseName()}`.`master_ttt`;";
                    await conn.ExecuteAsync(sql);

                    int batchSize = 100;

                    for (int i = 0; i < objTTT.Count; i += batchSize)
                    {
                        var batch = objTTT.Skip(i).Take(batchSize);

                        var values = string.Join(",", batch.Select(p =>
                            $"('{Escape(p.TAT_ID)}','{Escape(p.KD_TOKO)}','{Escape(p.KD_TOKO_PENERIMA)}'," +
                            $"'{tglStatus:yyyy-MM-dd HH:mm:ss}','{tglProses:yyyy-MM-dd HH:mm:ss}',USER(),NOW())"
                        ));

                        sql = $@"
                            INSERT INTO `{db.DatabaseName()}`.`master_ttt`
                            (`TAT_ID`,`KD_TOKO`,`KD_TOKO_PENERIMA`,
                             `TGL_STATUS`,`TGL_PROSES`,`UPDID_CABANG`,`UPDTIME_CABANG`)
                            VALUES {values};";

                        await conn.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertMasterTAT(string jobs, List<TAT> objTAT, DateTime tglStatus, DateTime tglProses)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                // CREATE TABLE
                sql = $@"
                        CREATE TABLE IF NOT EXISTS `{db.DatabaseName()}`.`master_tat` (
                         `TAT_ID` VARCHAR(50) NOT NULL DEFAULT '',
                         `KD_PLU` VARCHAR(10) NOT NULL DEFAULT '',
                         `TGL_AWAL` VARCHAR(20) NOT NULL DEFAULT '',
                         `TGL_AKHIR` VARCHAR(20) NOT NULL DEFAULT '',
                         `FLAG_PROD` VARCHAR(200) DEFAULT '',
                         `TGL_STATUS` DATETIME DEFAULT NULL,
                         `TGL_PROSES` DATETIME DEFAULT NULL,
                         `UPDID_CABANG` VARCHAR(50) DEFAULT NULL,
                         `UPDTIME_CABANG` DATETIME DEFAULT NULL,
                         PRIMARY KEY (`TAT_ID`,`KD_PLU`,`TGL_AWAL`,`TGL_AKHIR`)
                        ) ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                if (objTAT.Count > 0)
                {
                    sql = $"TRUNCATE `{db.DatabaseName()}`.`master_tat`;";
                    await conn.ExecuteAsync(sql);

                    int batchSize = 100;

                    for (int i = 0; i < objTAT.Count; i += batchSize)
                    {
                        var batch = objTAT.Skip(i).Take(batchSize);

                        var values = string.Join(",", batch.Select(p =>
                            $"('{Escape(p.TAT_ID)}','{Escape(p.KD_PLU)}','{Escape(p.TGL_AWAL)}','{Escape(p.TGL_AKHIR)}','{Escape(p.FLAG_PROD)}'," +
                            $"'{tglStatus:yyyy-MM-dd HH:mm:ss}','{tglProses:yyyy-MM-dd HH:mm:ss}',USER(),NOW())"
                        ));

                        sql = $@"
                                INSERT INTO `{db.DatabaseName()}`.`master_tat`
                                (`TAT_ID`,`KD_PLU`,`TGL_AWAL`,`TGL_AKHIR`,`FLAG_PROD`,
                                 `TGL_STATUS`,`TGL_PROSES`,`UPDID_CABANG`,`UPDTIME_CABANG`)
                                VALUES {values};";

                        await conn.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertMasterTokoProdSus(string jobs, List<TOKO_PRODSUS> objData, DateTime tglStatus, DateTime tglProses)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                // CREATE TABLE
                sql = $@"
                        CREATE TABLE IF NOT EXISTS `{db.DatabaseName()}`.`master_toko_prodsus` (
                         `ID_DATA` BIGINT(25) NOT NULL AUTO_INCREMENT,
                         `KD_CABANG` VARCHAR(10) NOT NULL DEFAULT '',
                         `NM_CABANG` VARCHAR(25) DEFAULT '',
                         `KD_TOKO` VARCHAR(10) NOT NULL DEFAULT '',
                         `NM_TOKO` VARCHAR(50) DEFAULT '',
                         `FLAG_TOKO` VARCHAR(10) DEFAULT '',
                         `CK_KIRIM` VARCHAR(20) DEFAULT '',
                         `TGL_STATUS` DATETIME DEFAULT NULL,
                         `TGL_PROSES` DATETIME DEFAULT NULL,
                         `UPDID_CABANG` VARCHAR(50) DEFAULT NULL,
                         `UPDTIME_CABANG` DATETIME DEFAULT NULL,
                         PRIMARY KEY (`ID_DATA`,`KD_CABANG`,`KD_TOKO`)
                        ) ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                if (objData.Count > 0)
                {
                    sql = $"TRUNCATE `{db.DatabaseName()}`.`master_toko_prodsus`;";
                    await conn.ExecuteAsync(sql);

                    int batchSize = 100;

                    for (int i = 0; i < objData.Count; i += batchSize)
                    {
                        var batch = objData.Skip(i).Take(batchSize);

                        var values = string.Join(",", batch.Select(p =>
                            $"('{Escape(p.KD_CABANG)}','{Escape(p.NM_CABANG)}','{Escape(p.KD_TOKO)}'," +
                            $"'{Escape(p.NM_TOKO)}','{Escape(p.FLAG_TOKO)}','{Escape(p.CK_KIRIM)}'," +
                            $"'{tglStatus:yyyy-MM-dd HH:mm:ss}','{tglProses:yyyy-MM-dd HH:mm:ss}',USER(),NOW())"
                        ));

                        sql = $@"
                        INSERT INTO `{db.DatabaseName()}`.`master_toko_prodsus`
                        (`KD_CABANG`,`NM_CABANG`,`KD_TOKO`,`NM_TOKO`,`FLAG_TOKO`,`CK_KIRIM`,
                         `TGL_STATUS`,`TGL_PROSES`,`UPDID_CABANG`,`UPDTIME_CABANG`)
                        VALUES {values};";

                        await conn.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertMasterProdSusJualToko(string jobs, List<PRODSUS_JUAL_TOKO> objData, DateTime tglStatus, DateTime tglProses)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                sql = $@"
                        CREATE TABLE IF NOT EXISTS `{db.DatabaseName()}`.`master_prodsus_jual_toko` (
                         `ID_DATA` BIGINT(25) NOT NULL AUTO_INCREMENT,
                         `KD_TOKO` VARCHAR(10) NOT NULL DEFAULT '',
                         `NM_TOKO` VARCHAR(50) DEFAULT '',
                         `KD_CABANG` VARCHAR(10) NOT NULL DEFAULT '',
                         `MERK` VARCHAR(50) DEFAULT '',
                         `PLU` VARCHAR(20) NOT NULL DEFAULT '',
                         `NM_PRODUK` VARCHAR(100) DEFAULT '',
                         `TGL_AKTIF_MERK` VARCHAR(25) DEFAULT '',
                         `TGL_STATUS` DATETIME DEFAULT NULL,
                         `TGL_PROSES` DATETIME DEFAULT NULL,
                         `UPDID_CABANG` VARCHAR(50) DEFAULT NULL,
                         `UPDTIME_CABANG` DATETIME DEFAULT NULL,
                         PRIMARY KEY (`ID_DATA`,`KD_TOKO`,`KD_CABANG`,`PLU`)
                        ) ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                if (objData.Count > 0)
                {
                    sql = $"TRUNCATE `{db.DatabaseName()}`.`master_prodsus_jual_toko`;";
                    await conn.ExecuteAsync(sql);

                    int batchSize = 100;

                    for (int i = 0; i < objData.Count; i += batchSize)
                    {
                        var batch = objData.Skip(i).Take(batchSize);

                        var values = string.Join(",", batch.Select(p =>
                            $"('{Escape(p.KD_TOKO)}','{Escape(p.NM_TOKO)}','{Escape(p.KD_CABANG)}'," +
                            $"'{Escape(p.MERK)}','{Escape(p.PLU)}','{Escape(p.NM_PRODUK)}','{Escape(p.TGL_AKTIF_MERK)}'," +
                            $"'{tglStatus:yyyy-MM-dd HH:mm:ss}','{tglProses:yyyy-MM-dd HH:mm:ss}',USER(),NOW())"
                        ));

                        sql = $@"
                            INSERT INTO `{db.DatabaseName()}`.`master_prodsus_jual_toko`
                            (`KD_TOKO`,`NM_TOKO`,`KD_CABANG`,`MERK`,`PLU`,`NM_PRODUK`,`TGL_AKTIF_MERK`,
                             `TGL_STATUS`,`TGL_PROSES`,`UPDID_CABANG`,`UPDTIME_CABANG`)
                            VALUES {values};";

                        await conn.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertMasterTokoProdSusPeriode(string jobs, List<TOKO_PRODSUS_PERIODE> objData, DateTime tglStatus, DateTime tglProses)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                sql = $@"
                        CREATE TABLE IF NOT EXISTS `{db.DatabaseName()}`.`master_toko_prodsus_periode` (
                         `ID_DATA` BIGINT(25) NOT NULL AUTO_INCREMENT,
                         `KD_TOKO` VARCHAR(10) NOT NULL DEFAULT '',
                         `NM_TOKO` VARCHAR(50) DEFAULT '',
                         `KD_CABANG` VARCHAR(10) NOT NULL DEFAULT '',
                         `HARI` VARCHAR(50) DEFAULT '',
                         `PERIODE_JUAL` VARCHAR(5) NOT NULL DEFAULT '',
                         `JAM_AWAL` VARCHAR(10) DEFAULT '',
                         `JAM_AKHIR` VARCHAR(10) DEFAULT '',
                         `TGL_STATUS` DATETIME DEFAULT NULL,
                         `TGL_PROSES` DATETIME DEFAULT NULL,
                         `UPDID_CABANG` VARCHAR(50) DEFAULT NULL,
                         `UPDTIME_CABANG` DATETIME DEFAULT NULL,
                         PRIMARY KEY (`ID_DATA`,`KD_TOKO`,`KD_CABANG`,`HARI`,`PERIODE_JUAL`)
                        ) ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await conn.ExecuteAsync(sql);

                if (objData.Count > 0)
                {
                    sql = $"TRUNCATE `{db.DatabaseName()}`.`master_toko_prodsus_periode`;";
                    await conn.ExecuteAsync(sql);

                    int batchSize = 100;

                    for (int i = 0; i < objData.Count; i += batchSize)
                    {
                        var batch = objData.Skip(i).Take(batchSize);

                        var values = string.Join(",", batch.Select(p =>
                            $"('{Escape(p.KD_TOKO)}','{Escape(p.NM_TOKO)}','{Escape(p.KD_CABANG)}'," +
                            $"'{Escape(p.HARI)}','{Escape(p.PERIODE_JUAL)}','{Escape(p.JAM_AWAL)}','{Escape(p.JAM_AKHIR)}'," +
                            $"'{tglStatus:yyyy-MM-dd HH:mm:ss}','{tglProses:yyyy-MM-dd HH:mm:ss}',USER(),NOW())"
                        ));

                        sql = $@"
                                INSERT INTO `{db.DatabaseName()}`.`master_toko_prodsus_periode`
                                (`KD_TOKO`,`NM_TOKO`,`KD_CABANG`,`HARI`,`PERIODE_JUAL`,`JAM_AWAL`,`JAM_AKHIR`,
                                 `TGL_STATUS`,`TGL_PROSES`,`UPDID_CABANG`,`UPDTIME_CABANG`)
                                VALUES {values};";

                        await conn.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertLogMaster(string jobs, string jenis, string tglStatus, string tglProses)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                string table = "";
                string kolom = "";

                switch (jenis.ToUpper())
                {
                    case "PROMO":
                        table = "partisipan_promo";
                        kolom = "KD_TOKO";
                        break;
                    case "PAJAK":
                        table = "partisipan_pajak";
                        kolom = "TOK_CODE";
                        break;
                    case "SUPPLIER":
                        table = "partisipan_supplier";
                        kolom = "KDTK";
                        break;
                    case "TKX":
                        table = "master_all_tkx";
                        kolom = "KDTK";
                        break;
                    case "DCX":
                        table = "master_all_dcx";
                        kolom = "KODE_TOKO";
                        break;
                    case "SARANA_PRODUK_KHUSUS":
                        table = "master_sarana_produk_khusus";
                        kolom = "TOK_CODE";
                        break;
                }

                sql = $@"SELECT DISTINCT {kolom} AS KD_TOKO, TGL_STATUS FROM `{db.DatabaseName()}`.`{table}`  WHERE TGL_PROSES = '{tglProses}';";

                var data = (await conn.QueryAsync<(string KD_TOKO, string TGL_STATUS)>(sql)).ToList();

                if (data.Count > 0)
                {
                    var values = string.Join(",", data.Select(d =>
                    {
                        long idToko = ConvertToIdToko(d.KD_TOKO);

                        return $"('{jenis}','{tglStatus}','{tglProses}'," +
                               $"'{d.KD_TOKO}',{idToko},'OK',NULL,USER(),NOW())";
                    }));

                    sql = $@"
                            INSERT IGNORE INTO `{db.DatabaseName()}`.`log_master`
                            (`JENIS`,`TGL_STATUS`,`TGL_PROSES`,
                             `KODETOKO`,`IDTOKO`,
                             `GET`,`SEND`,
                             `ADDID`,`ADDTIME`)
                            VALUES {values};";

                    await conn.ExecuteAsync(sql);
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertLogMasterV2(string jobs, string jenis, string tglStatus, string tglProses, string kodeToko, string idToko, string statusGet, string statusSend)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                string sendValue = string.IsNullOrWhiteSpace(statusSend)
                    ? "NULL"
                    : $"'{statusSend}'";

                sql = $@"
                        INSERT IGNORE INTO `{db.DatabaseName()}`.`log_master`
                        (`JENIS`,`TGL_STATUS`,`TGL_PROSES`,
                         `KODETOKO`,`IDTOKO`,
                         `GET`,`SEND`,
                         `ADDID`,`ADDTIME`)
                        VALUES
                        ('{jenis}','{tglStatus}','{tglProses}',
                         '{kodeToko}','{idToko}',
                         '{statusGet}',{sendValue},
                         USER(),NOW());";

                await conn.ExecuteAsync(sql);

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertLogMasterByListToko(string jobs, string jenis, string tglStatus, string tglProses, string statusGet, string statusSend)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();

                string SchemaMasterToko = string.Empty;
                string TableMasterToko = string.Empty;
                string schema = "";
                string table = "";

                //if (Connection.MasterToko.Contains("|"))
                //{
                //    var split = Connection.MasterToko.Split('|');
                //    schema = split[0];
                //    table = split[1];
                //}

                sql = $"SELECT KodeToko FROM `{schema}`.`{table}`;";
                var data = (await conn.QueryAsync<string>(sql)).ToList();

                if (data.Count > 0)
                {
                    int batchSize = 100;

                    for (int i = 0; i < data.Count; i += batchSize)
                    {
                        var batch = data.Skip(i).Take(batchSize);

                        var values = string.Join(",", batch.Select(kode =>
                        {
                            string kodeUpper = kode.ToUpper();
                            long idToko = ConvertToIdToko(kodeUpper);

                            string sendVal = string.IsNullOrWhiteSpace(statusSend)
                                ? "NULL"
                                : $"'{statusSend}'";

                            return $"('{jenis}','{tglStatus}','{tglProses}'," +
                                   $"'{kodeUpper}',{idToko},'{statusGet}',{sendVal},USER(),NOW())";
                        }));

                        sql = $@"
                                INSERT IGNORE INTO `{db.DatabaseName()}`.`log_master`
                                (`JENIS`,`TGL_STATUS`,`TGL_PROSES`,
                                 `KODETOKO`,`IDTOKO`,
                                 `GET`,`SEND`,
                                 `ADDID`,`ADDTIME`)
                                VALUES {values};";

                        await conn.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs,
                    ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);

                return false;
            }
        }

        public async Task<bool> InsertLogMasterByListTokoExtended(string jobs, string jenis, string tglStatus, string tglProses, string statusGet, string statusSend, string kodeCabang)
        {
            try
            {
                using var conn = db.CreateConnection();
                var db_ = db.DatabaseName();
                string MasterToko = string.Empty;
                string schemaMasterToko = string.Empty;
                string tableMasterToko = string.Empty;

                // ── Cek apakah toko_extended ada ─────────────────────────────
                var countTokoExtended = await conn.ExecuteScalarAsync<int>(
                    $"SELECT COUNT(TABLE_NAME) FROM INFORMATION_SCHEMA.TABLES" +
                    $" WHERE TABLE_SCHEMA = '{db_}' AND TABLE_NAME = 'toko_extended';");

                List<string> listToko;
                if (countTokoExtended == 0)
                {
                    //var schemaMasterToko = Connection.MasterToko.Contains("|")
                    //    ? Connection.MasterToko.Split('|')[0] : "";
                    //var tableMasterToko = Connection.MasterToko.Contains("|")
                    //    ? Connection.MasterToko.Split('|')[1] : Connection.MasterToko;

                    listToko = (await conn.QueryAsync<string>(
                        $"SELECT `KodeToko` FROM `{schemaMasterToko}`.`{tableMasterToko}`;")).ToList();
                }
                else
                {
                    listToko = (await conn.QueryAsync<string>(
                        $"SELECT `KodeToko` FROM `{db_}`.`toko_extended` WHERE `KodeGudang` = @kodeCabang;",
                        new { kodeCabang })).ToList();
                }

                if (listToko.Count == 0) return true;

                const string header =
                    "INSERT IGNORE INTO `{0}`.`log_master` (" +
                    "`JENIS`,`TGL_STATUS`,`TGL_PROSES`," +
                    "`KODETOKO`,`IDTOKO`," +
                    "`GET`,`SEND`," +
                    "`ADDID`,`ADDTIME`) VALUES ";

                await ExecuteBatchInsert(conn, listToko, batchSize: 100,
                    header: string.Format(header, db_),
                    onDuplicate: ";",
                    rowBuilder: kodeToko =>
                    {
                        // Convert KodeToko string ke numeric ID — sama dengan VB asli
                        var tokoUpper = kodeToko.ToUpper();
                        var convertStr = string.Concat(tokoUpper.Select(c => ((int)c).ToString()));
                        var idToko = long.Parse(convertStr);

                        var sendVal = string.IsNullOrWhiteSpace(statusSend)
                            ? "NULL"
                            : $"'{statusSend}'";

                        return $"('{jenis}','{tglStatus}','{tglProses}'," +
                               $"'{tokoUpper}','{idToko}'," +
                               $"'{statusGet}',{sendVal}," +
                               $"USER(),NOW())";
                    });

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> UpdateLogMaster(string jobs, string jenis, string tglStatus, string tglProses,  string kodeToko, int idToko, string sGet, string sSend)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                var db_ = db.DatabaseName();

                sql = $"UPDATE `{db_}`.`log_master`" +
                      $" SET `SEND` = @sSend, UPDID = USER(), UPDTIME = NOW()" +
                      $" WHERE JENIS = @jenis" +
                      $" AND TGL_STATUS = @tglStatus" +
                      $" AND TGL_PROSES = @tglProses" +
                      $" AND KODETOKO = @kodeToko";
                sql += idToko == 0 ? ";" : " AND IDTOKO = @idToko;";

                await conn.ExecuteAsync(sql, new { sSend, jenis, tglStatus, tglProses, kodeToko, idToko });
                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> UpdateConst(string jobs, string rKey, string desc, DateTime period, DateTime addTime)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                sql = "INSERT INTO `poscabang`.`const`" +
                      " (`RKEY`,`DESC`,`PERIOD`,`ADDTIME`) VALUES" +
                      " (@rKey, @desc, @period, @addTime)" +
                      " ON DUPLICATE KEY UPDATE `DESC`=VALUES(`DESC`), `ADDTIME`=VALUES(`ADDTIME`);";

                await conn.ExecuteAsync(sql, new
                {
                    rKey,
                    desc,
                    period = period.ToString("yyyy-MM-dd"),
                    addTime = addTime.ToString("yyyy-MM-dd HH:mm:ss")
                });
                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> CheckValidMasterAndPartisipanTrPr(string jobs, string kodeCabang)
        {
            string EmailTo = string.Empty;
            string EmailCC = string.Empty;
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                var db_ = db.DatabaseName();

                sql = $"SELECT DISTINCT KD_PROMO FROM `{db_}`.`partisipan_promo`" +
                      $" WHERE KD_PROMO NOT IN (SELECT DISTINCT KODEPROMO FROM `{db_}`.`master_all_promo`);";

                var invalidPromos = (await conn.QueryAsync<string>(sql)).ToList();

                if (invalidPromos.Count > 0)
                {
                    var listKdPromo = string.Join(", ", invalidPromos);
                    _objUtil.SendEmail(
                        EmailTo, EmailCC,
                        $"[WARNING] TrPrCabang.exe - {kodeCabang}",
                        $"[PROMO] Data MASTER dengan PARTISIPAN di WRC {kodeCabang.ToUpper()} tidak sesuai/valid.\r\n" +
                        $"TOTAL KODE PROMO SELISIH : {invalidPromos.Count}\r\n" +
                        $"LIST KODE PROMO SELISIH  : {listKdPromo}");
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                return false;
            }
        }

        public async Task<bool> InsertMasterTokoExtended(List<TOKO_EXTENDED> listTokoExtended)
        {
            string sql = "";
            try
            {
                using var conn = db.CreateConnection();
                var db_ = db.DatabaseName();

                // ── Create + Truncate temp_toko_extended ──────────────────────
                sql = $"CREATE TABLE IF NOT EXISTS `{db_}`.`temp_toko_extended` (" +
                      " `Tok_Id` VARCHAR(10) DEFAULT NULL," +
                      " `KodeToko` VARCHAR(8) NOT NULL DEFAULT ''," +
                      " `NamaToko` VARCHAR(100) DEFAULT NULL," +
                      " `KodeGudang` VARCHAR(8) NOT NULL DEFAULT ''," +
                      " `TglBuka` DATE DEFAULT NULL," +
                      " `AddID` VARCHAR(45) DEFAULT NULL," +
                      " `AddTime` DATETIME DEFAULT NULL," +
                      " PRIMARY KEY (`KodeToko`)," +
                      " KEY `IDX1` (`KodeGudang`)" +
                      ") ENGINE=INNODB DEFAULT CHARSET=latin1;" +
                      $"TRUNCATE `{db_}`.`temp_toko_extended`;";
                await conn.ExecuteAsync(sql);

                // ── Batch INSERT temp_toko_extended ───────────────────────────
                await ExecuteBatchInsert(conn, listTokoExtended, batchSize: 200,
                    header: $"INSERT INTO `{db_}`.`temp_toko_extended`" +
                            " (`Tok_Id`,`KodeToko`,`NamaToko`,`KodeGudang`,`TglBuka`,`AddID`,`AddTime`) VALUES ",
                    onDuplicate: ";",
                    rowBuilder: tok =>
                    {
                        string V(string? val) => (val ?? "").Replace("'", "''").Replace("\\", "\\\\");
                        return $"('{V(tok.Tok_ID)}','{V(tok.KODE_TOKO)}','{V(tok.NAMA_TOKO)}'," +
                               $"'{V(tok.KODE_GUDANG)}','{tok.TGL_BUKA}',USER(),NOW())";
                    });

                // ── Recreate toko_extended dari temp ─────────────────────────
                await conn.ExecuteAsync(
                    $"DROP TABLE IF EXISTS `{db_}`.`toko_extended`;" +
                    $"CREATE TABLE `{db_}`.`toko_extended` SELECT * FROM `{db_}`.`temp_toko_extended`;");

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog("TOK", ex.Message + "\r\n" + ex.StackTrace + "\r\nSql: " + sql,
                    Utility.TipeLog.Error);
                return false;
            }
        }

        // helper
        private string Escape(object val)
        {
            return val?.ToString().Replace("'", "''").Replace("\\", "\\\\") ?? "";
        }
        private long ConvertToIdToko(string kode)
        {
            var sb = new StringBuilder();

            foreach (char c in kode)
            {
                sb.Append((int)c);
            }

            return long.Parse(sb.ToString());
        }

        // ---------------------------------------------------------------------------------------------------------------------------------------------

        // ── Helper: Generic Batch Insert ─────────────────────────────────────────
        private async Task ExecuteBatchInsert<T>(IDbConnection conn, List<T> items, int batchSize, string header, string onDuplicate, Func<T, string> rowBuilder)
        {
            var sqlBuild = new StringBuilder();
            int count = 0;

            sqlBuild.Append(header);

            foreach (var item in items)
            {
                sqlBuild.Append(rowBuilder(item));
                sqlBuild.Append(',');
                count++;

                if (count == batchSize)
                {
                    await FlushBatch(conn, sqlBuild, onDuplicate);
                    count = 0;
                    sqlBuild.Clear();
                    sqlBuild.Append(header);
                }
            }

            if (count > 0)
                await FlushBatch(conn, sqlBuild, onDuplicate);
        }

        private static async Task FlushBatch(IDbConnection conn, StringBuilder sqlBuild, string onDuplicate)
        {
            sqlBuild.Length--; // hapus trailing comma
            sqlBuild.Append(onDuplicate);
            await conn.ExecuteAsync(sqlBuild.ToString());
        }

        // ── Helpers ───────────────────────────────────────────────────────────────
        private async Task AlterIfColumnNotExists(IDbConnection conn, string schema, string table, string column, string alterSql, bool dropIfMissing = false)
        {
            try
            {
                var count = await conn.ExecuteScalarAsync<int>(
                    "SELECT COUNT(*) FROM `information_schema`.`COLUMNS`" +
                    " WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @column;",
                    new { schema, table, column });

                if (count == 0)
                {
                    _objUtil.Tracelog("InitialTables",
                        $"[ALTER] {schema}.{table} — kolom '{column}' tidak ada, menjalankan: {alterSql}",
                        Utility.TipeLog.Debug);
                    await conn.ExecuteAsync(alterSql);
                }
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog("InitialTables", $"[ALTER GAGAL] {schema}.{table}.{column}\n{ex.Message}\nSql: {alterSql}",Utility.TipeLog.Error);
            }
        }
        private async Task AlterIfColumnNotExistsWithKey(IDbConnection conn, string schema, string table, string column, string columnKey, string alterSql)
        {
            try
            {
                var count = await conn.ExecuteScalarAsync<int>(
                    "SELECT COUNT(*) FROM `information_schema`.`COLUMNS`" +
                    " WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table" +
                    " AND COLUMN_NAME = @column AND COLUMN_KEY = @columnKey;",
                    new { schema, table, column, columnKey });

                if (count == 0)
                {
                    _objUtil.Tracelog("InitialTables",
                        $"[ALTER] {schema}.{table} — kolom '{column}' dengan key '{columnKey}' tidak ada, menjalankan: {alterSql}",
                        Utility.TipeLog.Debug);
                    await conn.ExecuteAsync(alterSql);
                }
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog("InitialTables", $"[ALTER GAGAL] {schema}.{table}.{column} (key: {columnKey})\n{ex.Message}\nSql: {alterSql}", Utility.TipeLog.Error);
            }
        }

        // Overload tanpa indexName — cek hanya berdasarkan column
        private async Task AlterIfIndexNotExists(IDbConnection conn, string schema, string table, string column, string alterSql)
            => await AlterIfIndexNotExists(conn, schema, table, column, null, alterSql);

        private async Task AlterIfIndexNotExists(IDbConnection conn, string schema, string table,string column, string? indexName, string alterSql)
        {
            try
            {
                var sql = "SELECT COUNT(*) FROM `information_schema`.`STATISTICS`" +
                          " WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @table AND COLUMN_NAME = @column";
                if (indexName != null) sql += " AND INDEX_NAME = @indexName";
                sql += ";";

                var count = await conn.ExecuteScalarAsync<int>(sql, new { schema, table, column, indexName });
                if (count == 0)
                    await conn.ExecuteAsync(alterSql);
            }
            catch { }
        }



    }
}
