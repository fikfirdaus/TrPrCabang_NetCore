using Dapper;
using MySqlConnector;
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
    public class PosRTDA
    {
        private readonly Utility _objUtil;
        private readonly ServiceController _objSvr;
        private readonly CompressHelper _objComp;
        private readonly IDbServices db;

        public PosRTDA(IDbServices db)
        {
            this.db = db;
        }

        private string ConvertSQL(string jobs, string queryBasic, string queryModif, ref string queryFinal)
        {
            bool rtn;
            try
            {
                string temp = queryBasic.Replace("'", "''");
                string[] arrTemp = queryModif.Split('^');
                for (int i = 0; i < arrTemp.Length; i++)
                {
                    if (i == arrTemp.Length - 1)
                        queryFinal += arrTemp[i];
                    else
                        queryFinal += arrTemp[i] + temp;
                }
                rtn = true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                rtn = false;
            }
            return rtn.ToString();
        }

        public async Task<bool> InsertTrPrPromoTask(string jobs, DataTable dtPromo, string kodeToko, DateTime tglStatus, Config conf, int idThreadDoWork)
        {
            bool rtn = false;
            string msgSvr = "";
            var sqlBuild = new StringBuilder();
            string sqlTask = "";
            string judulTask = "";
            var dateNow = DateTime.Now;
            int countIns = 1000000;
            int iCount = 0;
            int iTask = 0;
            byte[] tempByte = null;
            string tempKodePromo = "";
            double sizeCmd = 0;
            string tglStatusStr = tglStatus.ToString("yyMMdd");

            try
            {
                if (dtPromo.Rows.Count > 0)
                {
                    // Revisi (26 Januari 2024) : Tambah kolom HARIED - Memo 1800/CPS/23
                    // Revisi (15 Juli 2024) : Tambah field PRO_DESC_PRICE_TAG
                    // Revisi (12 Agustus 2024) : Tambah field PRO_JENIS_SHELFTALKER
                    sqlBuild.Append("SET GLOBAL max_allowed_packet = 1073741824 * 2;");
                    sqlBuild.Append($" CREATE TABLE IF NOT EXISTS `pos`.`trpr_promo_ws{tglStatusStr}` (");
                    sqlBuild.Append(" `KODEPROMO` varchar(10) NOT NULL DEFAULT '',");
                    sqlBuild.Append(" `SUBKODEPROMO` varchar(45) NOT NULL DEFAULT '',");
                    sqlBuild.Append(" `TIPEPROMO` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `KODEGROUP` varchar(10) NOT NULL DEFAULT '',");
                    sqlBuild.Append(" `MEKANISME` varchar(2000) DEFAULT NULL,");
                    sqlBuild.Append(" `CETAKSTRUK1` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `CETAKSTRUK2` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `CETAKSTRUK3` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `CETAKLAYAR1` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `CETAKLAYAR2` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `CETAKLAYAR3` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `TANGGALAWAL` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `TANGGALAKHIR` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `PERIODEJAM` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `PERIODEMINGGUAN` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `PERIODEBULANAN` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `ITEMSYARAT` longtext,");
                    sqlBuild.Append(" `QTYSYARATMIN` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `QTYSYARATMAX` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `QTYSYARATTAMBAH` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `RPSYARATMIN` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `RPSYARATMAX` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `RPSYARATTAMBAH` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `ITEMTARGET` longtext,");
                    sqlBuild.Append(" `QTYTARGET` longtext,");
                    sqlBuild.Append(" `QTYTARGETMAX` longtext,");
                    sqlBuild.Append(" `QTYTARGETTAMBAH` longtext,");
                    sqlBuild.Append(" `RPTARGET` longtext,");
                    sqlBuild.Append(" `RPTARGETMAX` longtext,");
                    sqlBuild.Append(" `RPTARGETTAMBAH` longtext,");
                    sqlBuild.Append(" `POTONGANPERSENTARGET` longtext,");
                    sqlBuild.Append(" `KODEBIN` longtext,");
                    sqlBuild.Append(" `KODEMEMBER` longtext,");
                    sqlBuild.Append(" `BERSYARAT` varchar(1) DEFAULT NULL,");
                    sqlBuild.Append(" `KODESHIFT` varchar(1) DEFAULT NULL,");
                    sqlBuild.Append(" `NOSTRUK` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `SYARATCAMPUR` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `TRANSAKSIMAX` longtext,");
                    sqlBuild.Append(" `QTYTARGETMAXPROMO` longtext,");
                    sqlBuild.Append(" `RPTARGETMAXPROMO` longtext,");
                    sqlBuild.Append(" `TARGETTDKDIJUAL` longtext,");
                    sqlBuild.Append(" `STICKER` varchar(1) DEFAULT NULL,");
                    sqlBuild.Append(" `PEMBATASAN_HADIAH` varchar(1) DEFAULT NULL,");
                    sqlBuild.Append(" `TENGGANGWAKTU` longtext,");
                    sqlBuild.Append(" `QTYMAXPERSTRUK` longtext,");
                    sqlBuild.Append(" `HIGHIMPACT` varchar(3) DEFAULT NULL,");
                    sqlBuild.Append(" `POTONGMARGIN` varchar(500) DEFAULT NULL,");
                    sqlBuild.Append(" `MAXRPBELANJA` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `TGL_UPDATE` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `NOURUT` bigint(20) DEFAULT NULL,");
                    sqlBuild.Append(" `QTY_TBS_MRH` longtext,");
                    sqlBuild.Append(" `KEYPROMOSI` varchar(500) DEFAULT '',");
                    sqlBuild.Append(" `IKIOS` varchar(1) DEFAULT '',");
                    sqlBuild.Append(" `PRIORITAS` longtext,");
                    sqlBuild.Append(" `ID` DECIMAL(2,0) DEFAULT '1',");
                    sqlBuild.Append(" `I_STORE` varchar(1) DEFAULT '',");
                    sqlBuild.Append(" `SPECIAL_PRODUCT` varchar(1) DEFAULT '',");
                    sqlBuild.Append(" `PRO_SPECIAL_REQUEST` varchar(45) DEFAULT '',");
                    sqlBuild.Append(" `PRO_JENIS_SPECIAL_REQ` varchar(45) DEFAULT '',");
                    sqlBuild.Append(" `PRO_EXCLUDE_BIN` longtext,");
                    sqlBuild.Append(" `HARIED` varchar(20) DEFAULT '',");
                    sqlBuild.Append(" `PRO_DESC_PRICE_TAG` varchar(30) DEFAULT '',");
                    sqlBuild.Append(" `PRO_JENIS_SHELFTALKER` varchar(50) DEFAULT '',");
                    sqlBuild.Append(" `UPDID_TOKO` varchar(45) DEFAULT NULL,");
                    sqlBuild.Append(" `UPDTIME_TOKO` datetime DEFAULT NULL,");
                    sqlBuild.Append(" PRIMARY KEY (`KODEPROMO`,`SUBKODEPROMO`,`KODEGROUP`,`ID`)");
                    sqlBuild.Append(") ENGINE=INNODB DEFAULT CHARSET=latin1;");

                    sqlBuild.Append($"\nINSERT INTO `pos`.`trpr_promo_ws{tglStatusStr}` (");
                    sqlBuild.Append("`KODEPROMO`,`SUBKODEPROMO`,`TIPEPROMO`,");
                    sqlBuild.Append("`KODEGROUP`,`MEKANISME`,`CETAKSTRUK1`,");
                    sqlBuild.Append("`CETAKSTRUK2`,`CETAKSTRUK3`,`CETAKLAYAR1`,");
                    sqlBuild.Append("`CETAKLAYAR2`,`CETAKLAYAR3`,`TANGGALAWAL`,");
                    sqlBuild.Append("`TANGGALAKHIR`,`PERIODEJAM`,`PERIODEMINGGUAN`,");
                    sqlBuild.Append("`PERIODEBULANAN`,`ITEMSYARAT`,`QTYSYARATMIN`,");
                    sqlBuild.Append("`QTYSYARATMAX`,`QTYSYARATTAMBAH`,`RPSYARATMIN`,");
                    sqlBuild.Append("`RPSYARATMAX`,`RPSYARATTAMBAH`,`ITEMTARGET`,");
                    sqlBuild.Append("`QTYTARGET`,`QTYTARGETMAX`,`QTYTARGETTAMBAH`,");
                    sqlBuild.Append("`RPTARGET`,`RPTARGETMAX`,`RPTARGETTAMBAH`,");
                    sqlBuild.Append("`POTONGANPERSENTARGET`,`KODEBIN`,`KODEMEMBER`,");
                    sqlBuild.Append("`BERSYARAT`,`KODESHIFT`,`NOSTRUK`,");
                    sqlBuild.Append("`SYARATCAMPUR`,`TRANSAKSIMAX`,`QTYTARGETMAXPROMO`,");
                    sqlBuild.Append("`RPTARGETMAXPROMO`,`TARGETTDKDIJUAL`,`STICKER`,");
                    sqlBuild.Append("`PEMBATASAN_HADIAH`,`TENGGANGWAKTU`,`QTYMAXPERSTRUK`,");
                    sqlBuild.Append("`HIGHIMPACT`,`POTONGMARGIN`,`MAXRPBELANJA`,");
                    sqlBuild.Append("`TGL_UPDATE`,`NOURUT`,`QTY_TBS_MRH`,");
                    sqlBuild.Append("`KEYPROMOSI`,`IKIOS`,`PRIORITAS`,");
                    sqlBuild.Append("`ID`,`I_STORE`,`SPECIAL_PRODUCT`,");
                    sqlBuild.Append("`PRO_SPECIAL_REQUEST`,`PRO_JENIS_SPECIAL_REQ`,`PRO_EXCLUDE_BIN`,");
                    sqlBuild.Append("`HARIED`,`PRO_DESC_PRICE_TAG`,`PRO_JENIS_SHELFTALKER`,");
                    sqlBuild.Append("`UPDID_TOKO`,`UPDTIME_TOKO`) VALUES ");

                    foreach (DataRow pro in dtPromo.Rows)
                    {
                        tempKodePromo = pro["KODEPROMO"].ToString();
                        string V(string col) => pro[col].ToString().Replace("'", "''").Replace("\\", "\\\\");

                        sqlBuild.Append($"\n('{V("KODEPROMO")}','{V("SUBKODEPROMO")}','{V("TIPEPROMO")}'," +
                                        $"'{V("KODEGROUP")}','{V("MEKANISME")}','{V("CETAKSTRUK1")}'," +
                                        $"'{V("CETAKSTRUK2")}','{V("CETAKSTRUK3")}','{V("CETAKLAYAR1")}'," +
                                        $"'{V("CETAKLAYAR2")}','{V("CETAKLAYAR3")}','{V("TANGGALAWAL")}'," +
                                        $"'{V("TANGGALAKHIR")}','{V("PERIODEJAM")}','{V("PERIODEMINGGUAN")}'," +
                                        $"'{V("PERIODEBULANAN")}','{V("ITEMSYARAT")}','{V("QTYSYARATMIN")}'," +
                                        $"'{V("QTYSYARATMAX")}','{V("QTYSYARATTAMBAH")}','{V("RPSYARATMIN")}'," +
                                        $"'{V("RPSYARATMAX")}','{V("RPSYARATTAMBAH")}','{V("ITEMTARGET")}'," +
                                        $"'{V("QTYTARGET")}','{V("QTYTARGETMAX")}','{V("QTYTARGETTAMBAH")}'," +
                                        $"'{V("RPTARGET")}','{V("RPTARGETMAX")}','{V("RPTARGETTAMBAH")}'," +
                                        $"'{V("POTONGANPERSENTARGET")}','{V("KODEBIN")}','{V("KODEMEMBER")}'," +
                                        $"'{V("BERSYARAT")}','{V("KODESHIFT")}','{V("NOSTRUK")}'," +
                                        $"'{V("SYARATCAMPUR")}','{V("TRANSAKSIMAX")}','{V("QTYTARGETMAXPROMO")}'," +
                                        $"'{V("RPTARGETMAXPROMO")}','{V("TARGETTDKDIJUAL")}','{V("STICKER")}'," +
                                        $"'{V("PEMBATASAN_HADIAH")}','{V("TENGGANGWAKTU")}','{V("QTYMAXPERSTRUK")}'," +
                                        $"'{V("HIGHIMPACT")}','{V("POTONGMARGIN")}','{V("MAXRPBELANJA")}'," +
                                        $"'{V("TGL_UPDATE")}','{pro["NOURUT"]}','{V("QTY_TBS_MRH")}'," +
                                        $"'{V("KEYPROMOSI")}','{V("IKIOS")}','{V("PRIORITAS")}'," +
                                        $"{pro["ID"]},'{V("I_STORE")}','{V("SPECIAL_PRODUCT")}'," +
                                        $"'{V("PRO_SPECIAL_REQUEST")}','{V("PRO_JENIS_SPECIAL_REQ")}','{V("PRO_EXCLUDE_BIN")}'," +
                                        $"'{V("HARIED")}','{V("PRO_DESC_PRICE_TAG")}','{V("PRO_JENIS_SHELFTALKER")}'," +
                                        $"USER(), NOW()),");

                        iCount++;

                        if (iCount == countIns)
                        {
                            sqlBuild.Length--;
                            sqlBuild.Append(" ON DUPLICATE KEY UPDATE TIPEPROMO=VALUES(TIPEPROMO),");
                            sqlBuild.Append("MEKANISME=VALUES(MEKANISME),CETAKSTRUK1=VALUES(CETAKSTRUK1),");
                            sqlBuild.Append("CETAKSTRUK2=VALUES(CETAKSTRUK2),CETAKSTRUK3=VALUES(CETAKSTRUK3),");
                            sqlBuild.Append("CETAKLAYAR1=VALUES(CETAKLAYAR1),CETAKLAYAR2=VALUES(CETAKLAYAR2),");
                            sqlBuild.Append("CETAKLAYAR3=VALUES(CETAKLAYAR3),TANGGALAWAL=VALUES(TANGGALAWAL),");
                            sqlBuild.Append("TANGGALAKHIR=VALUES(TANGGALAKHIR),PERIODEJAM=VALUES(PERIODEJAM),");
                            sqlBuild.Append("PERIODEMINGGUAN=VALUES(PERIODEMINGGUAN),PERIODEBULANAN=VALUES(PERIODEBULANAN),");
                            sqlBuild.Append("ITEMSYARAT=VALUES(ITEMSYARAT),QTYSYARATMIN=VALUES(QTYSYARATMIN),");
                            sqlBuild.Append("QTYSYARATMAX=VALUES(QTYSYARATMAX),QTYSYARATTAMBAH=VALUES(QTYSYARATTAMBAH),");
                            sqlBuild.Append("RPSYARATMIN=VALUES(RPSYARATMIN),RPSYARATMAX=VALUES(RPSYARATMAX),");
                            sqlBuild.Append("RPSYARATTAMBAH=VALUES(RPSYARATTAMBAH),ITEMTARGET=VALUES(ITEMTARGET),");
                            sqlBuild.Append("QTYTARGET=VALUES(QTYTARGET),QTYTARGETMAX=VALUES(QTYTARGETMAX),");
                            sqlBuild.Append("QTYTARGETTAMBAH=VALUES(QTYTARGETTAMBAH),RPTARGET=VALUES(RPTARGET),");
                            sqlBuild.Append("RPTARGETMAX=VALUES(RPTARGETMAX),RPTARGETTAMBAH=VALUES(RPTARGETTAMBAH),");
                            sqlBuild.Append("POTONGANPERSENTARGET=VALUES(POTONGANPERSENTARGET),KODEBIN=VALUES(KODEBIN),");
                            sqlBuild.Append("KODEMEMBER=VALUES(KODEMEMBER),BERSYARAT=VALUES(BERSYARAT),");
                            sqlBuild.Append("KODESHIFT=VALUES(KODESHIFT),NOSTRUK=VALUES(NOSTRUK),");
                            sqlBuild.Append("SYARATCAMPUR=VALUES(SYARATCAMPUR),TRANSAKSIMAX=VALUES(TRANSAKSIMAX),");
                            sqlBuild.Append("QTYTARGETMAXPROMO=VALUES(QTYTARGETMAXPROMO),RPTARGETMAXPROMO=VALUES(RPTARGETMAXPROMO),");
                            sqlBuild.Append("TARGETTDKDIJUAL=VALUES(TARGETTDKDIJUAL),STICKER=VALUES(STICKER),");
                            sqlBuild.Append("PEMBATASAN_HADIAH=VALUES(PEMBATASAN_HADIAH),TENGGANGWAKTU=VALUES(TENGGANGWAKTU),");
                            sqlBuild.Append("QTYMAXPERSTRUK=VALUES(QTYMAXPERSTRUK),HIGHIMPACT=VALUES(HIGHIMPACT),");
                            sqlBuild.Append("POTONGMARGIN=VALUES(POTONGMARGIN),MAXRPBELANJA=VALUES(MAXRPBELANJA),");
                            sqlBuild.Append("TGL_UPDATE=VALUES(TGL_UPDATE),NOURUT=VALUES(NOURUT),QTY_TBS_MRH=VALUES(QTY_TBS_MRH),");
                            sqlBuild.Append("KEYPROMOSI=VALUES(KEYPROMOSI),IKIOS=VALUES(IKIOS),PRIORITAS=VALUES(PRIORITAS),");
                            sqlBuild.Append("ID=VALUES(ID),I_STORE=VALUES(I_STORE),SPECIAL_PRODUCT=VALUES(SPECIAL_PRODUCT),");
                            sqlBuild.Append("PRO_SPECIAL_REQUEST=VALUES(PRO_SPECIAL_REQUEST),PRO_JENIS_SPECIAL_REQ=VALUES(PRO_JENIS_SPECIAL_REQ),PRO_EXCLUDE_BIN=VALUES(PRO_EXCLUDE_BIN),");
                            sqlBuild.Append("HARIED=VALUES(HARIED),PRO_DESC_PRICE_TAG=VALUES(PRO_DESC_PRICE_TAG),PRO_JENIS_SHELFTALKER=VALUES(PRO_JENIS_SHELFTALKER),");
                            sqlBuild.Append("UPDID_TOKO=VALUES(UPDID_TOKO),UPDTIME_TOKO=VALUES(UPDTIME_TOKO);");

                            sqlTask = sqlBuild.ToString();
                            iTask++;
                            judulTask = $"TRPR_PROMOSI|{kodeToko.ToUpper()}|{tglStatusStr}|{dateNow:yyyyMMddHHmm}|{iTask}";

                            // Revisi (27 Oktober 2023) : tambah cek opsi pengiriman data ke server POSRT cabang
                            if (conf.IsPosRT_RestApi)
                            {
                                if (! await _objSvr.SendTrPrTask_RestApi(jobs, sqlTask, kodeToko.ToUpper(), judulTask, conf, msgSvr))
                                {
                                    sqlTask = null;
                                    goto ExitTry;
                                }
                            }
                            else
                            {
                                tempByte = _objComp.Compress(sqlTask);
                                sizeCmd = tempByte.Length / 1024.0;
                                if (! await _objSvr.SendTrPrTask_Service(jobs, tempByte, kodeToko.ToUpper(), judulTask, conf, msg => msgSvr = msg))
                                {
                                    sqlTask = null;
                                    goto ExitTry;
                                }
                            }

                            // Reset untuk batch berikutnya
                            iCount = 0;
                            sqlBuild.Clear();
                            sqlBuild.Append($"\nINSERT INTO `pos`.`trpr_promo_ws{tglStatusStr}` (");
                            sqlBuild.Append("`KODEPROMO`,`SUBKODEPROMO`,`TIPEPROMO`,");
                            sqlBuild.Append("`KODEGROUP`,`MEKANISME`,`CETAKSTRUK1`,");
                            sqlBuild.Append("`CETAKSTRUK2`,`CETAKSTRUK3`,`CETAKLAYAR1`,");
                            sqlBuild.Append("`CETAKLAYAR2`,`CETAKLAYAR3`,`TANGGALAWAL`,");
                            sqlBuild.Append("`TANGGALAKHIR`,`PERIODEJAM`,`PERIODEMINGGUAN`,");
                            sqlBuild.Append("`PERIODEBULANAN`,`ITEMSYARAT`,`QTYSYARATMIN`,");
                            sqlBuild.Append("`QTYSYARATMAX`,`QTYSYARATTAMBAH`,`RPSYARATMIN`,");
                            sqlBuild.Append("`RPSYARATMAX`,`RPSYARATTAMBAH`,`ITEMTARGET`,");
                            sqlBuild.Append("`QTYTARGET`,`QTYTARGETMAX`,`QTYTARGETTAMBAH`,");
                            sqlBuild.Append("`RPTARGET`,`RPTARGETMAX`,`RPTARGETTAMBAH`,");
                            sqlBuild.Append("`POTONGANPERSENTARGET`,`KODEBIN`,`KODEMEMBER`,");
                            sqlBuild.Append("`BERSYARAT`,`KODESHIFT`,`NOSTRUK`,");
                            sqlBuild.Append("`SYARATCAMPUR`,`TRANSAKSIMAX`,`QTYTARGETMAXPROMO`,");
                            sqlBuild.Append("`RPTARGETMAXPROMO`,`TARGETTDKDIJUAL`,`STICKER`,");
                            sqlBuild.Append("`PEMBATASAN_HADIAH`,`TENGGANGWAKTU`,`QTYMAXPERSTRUK`,");
                            sqlBuild.Append("`HIGHIMPACT`,`POTONGMARGIN`,`MAXRPBELANJA`,");
                            sqlBuild.Append("`TGL_UPDATE`,`NOURUT`,`QTY_TBS_MRH`,");
                            sqlBuild.Append("`KEYPROMOSI`,`IKIOS`,`PRIORITAS`,");
                            sqlBuild.Append("`ID`,`I_STORE`,`SPECIAL_PRODUCT`,");
                            sqlBuild.Append("`PRO_SPECIAL_REQUEST`,`PRO_JENIS_SPECIAL_REQ`,`PRO_EXCLUDE_BIN`,");
                            sqlBuild.Append("`HARIED`,`PRO_DESC_PRICE_TAG`,`PRO_JENIS_SHELFTALKER`,");
                            sqlBuild.Append("`UPDID_TOKO`,`UPDTIME_TOKO`) VALUES ");
                        }
                    }

                    if (iCount > 0)
                    {
                        sqlBuild.Length--;
                        sqlBuild.Append(" ON DUPLICATE KEY UPDATE TIPEPROMO=VALUES(TIPEPROMO),");
                        sqlBuild.Append("MEKANISME=VALUES(MEKANISME),CETAKSTRUK1=VALUES(CETAKSTRUK1),");
                        sqlBuild.Append("CETAKSTRUK2=VALUES(CETAKSTRUK2),CETAKSTRUK3=VALUES(CETAKSTRUK3),");
                        sqlBuild.Append("CETAKLAYAR1=VALUES(CETAKLAYAR1),CETAKLAYAR2=VALUES(CETAKLAYAR2),");
                        sqlBuild.Append("CETAKLAYAR3=VALUES(CETAKLAYAR3),TANGGALAWAL=VALUES(TANGGALAWAL),");
                        sqlBuild.Append("TANGGALAKHIR=VALUES(TANGGALAKHIR),PERIODEJAM=VALUES(PERIODEJAM),");
                        sqlBuild.Append("PERIODEMINGGUAN=VALUES(PERIODEMINGGUAN),PERIODEBULANAN=VALUES(PERIODEBULANAN),");
                        sqlBuild.Append("ITEMSYARAT=VALUES(ITEMSYARAT),QTYSYARATMIN=VALUES(QTYSYARATMIN),");
                        sqlBuild.Append("QTYSYARATMAX=VALUES(QTYSYARATMAX),QTYSYARATTAMBAH=VALUES(QTYSYARATTAMBAH),");
                        sqlBuild.Append("RPSYARATMIN=VALUES(RPSYARATMIN),RPSYARATMAX=VALUES(RPSYARATMAX),");
                        sqlBuild.Append("RPSYARATTAMBAH=VALUES(RPSYARATTAMBAH),ITEMTARGET=VALUES(ITEMTARGET),");
                        sqlBuild.Append("QTYTARGET=VALUES(QTYTARGET),QTYTARGETMAX=VALUES(QTYTARGETMAX),");
                        sqlBuild.Append("QTYTARGETTAMBAH=VALUES(QTYTARGETTAMBAH),RPTARGET=VALUES(RPTARGET),");
                        sqlBuild.Append("RPTARGETMAX=VALUES(RPTARGETMAX),RPTARGETTAMBAH=VALUES(RPTARGETTAMBAH),");
                        sqlBuild.Append("POTONGANPERSENTARGET=VALUES(POTONGANPERSENTARGET),KODEBIN=VALUES(KODEBIN),");
                        sqlBuild.Append("KODEMEMBER=VALUES(KODEMEMBER),BERSYARAT=VALUES(BERSYARAT),");
                        sqlBuild.Append("KODESHIFT=VALUES(KODESHIFT),NOSTRUK=VALUES(NOSTRUK),");
                        sqlBuild.Append("SYARATCAMPUR=VALUES(SYARATCAMPUR),TRANSAKSIMAX=VALUES(TRANSAKSIMAX),");
                        sqlBuild.Append("QTYTARGETMAXPROMO=VALUES(QTYTARGETMAXPROMO),RPTARGETMAXPROMO=VALUES(RPTARGETMAXPROMO),");
                        sqlBuild.Append("TARGETTDKDIJUAL=VALUES(TARGETTDKDIJUAL),STICKER=VALUES(STICKER),");
                        sqlBuild.Append("PEMBATASAN_HADIAH=VALUES(PEMBATASAN_HADIAH),TENGGANGWAKTU=VALUES(TENGGANGWAKTU),");
                        sqlBuild.Append("QTYMAXPERSTRUK=VALUES(QTYMAXPERSTRUK),HIGHIMPACT=VALUES(HIGHIMPACT),");
                        sqlBuild.Append("POTONGMARGIN=VALUES(POTONGMARGIN),MAXRPBELANJA=VALUES(MAXRPBELANJA),");
                        sqlBuild.Append("TGL_UPDATE=VALUES(TGL_UPDATE),NOURUT=VALUES(NOURUT),QTY_TBS_MRH=VALUES(QTY_TBS_MRH),");
                        sqlBuild.Append("KEYPROMOSI=VALUES(KEYPROMOSI),IKIOS=VALUES(IKIOS),PRIORITAS=VALUES(PRIORITAS),");
                        sqlBuild.Append("ID=VALUES(ID),I_STORE=VALUES(I_STORE),SPECIAL_PRODUCT=VALUES(SPECIAL_PRODUCT),");
                        sqlBuild.Append("PRO_SPECIAL_REQUEST=VALUES(PRO_SPECIAL_REQUEST),PRO_JENIS_SPECIAL_REQ=VALUES(PRO_JENIS_SPECIAL_REQ),PRO_EXCLUDE_BIN=VALUES(PRO_EXCLUDE_BIN),");
                        sqlBuild.Append("HARIED=VALUES(HARIED),PRO_DESC_PRICE_TAG=VALUES(PRO_DESC_PRICE_TAG),PRO_JENIS_SHELFTALKER=VALUES(PRO_JENIS_SHELFTALKER),");
                        sqlBuild.Append("UPDID_TOKO=VALUES(UPDID_TOKO),UPDTIME_TOKO=VALUES(UPDTIME_TOKO);");

                        sqlTask = sqlBuild.ToString();
                        iTask++;
                        judulTask = $"TRPR_PROMOSI|{kodeToko.ToUpper()}|{tglStatusStr}|{dateNow:yyyyMMddHHmm}|{iTask}";

                        // Revisi (27 Oktober 2023) : tambah cek opsi pengiriman data ke server POSRT cabang
                        if (conf.IsPosRT_RestApi)
                        {
                            if (!await _objSvr.SendTrPrTask_RestApi(jobs, sqlTask, kodeToko.ToUpper(), judulTask, conf, msgSvr))
                            {
                                sqlTask = null;
                                goto ExitTry;
                            }
                        }
                        else
                        {
                            tempByte = _objComp.Compress(sqlTask);
                            sizeCmd = tempByte.Length / 1024.0;
                            if (!await _objSvr.SendTrPrTask_Service(jobs, tempByte, kodeToko.ToUpper(), judulTask, conf, msg => msgSvr = msg))
                            {
                                sqlTask = null;
                                goto ExitTry;
                            }
                        }
                    }

                    _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|SUKSES SEND DATA TASK [{kodeToko}][{judulTask}]|SizeData: {sizeCmd} KB", Utility.TipeLog.Debug);
                    rtn = true;
                }
                else
                {
                    _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|GAGAL SEND DATA TASK [{kodeToko}][{judulTask}]|Data Promo Kosong", Utility.TipeLog.Debug);
                    rtn = false;
                }

            ExitTry:;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|ERROR - InsertTrPrPromoTask [{kodeToko}|{tempKodePromo}|{tglStatus}]\r\n{ex.Message}\r\n{ex.StackTrace}", Utility.TipeLog.Error);
                _objUtil.Tracelog(jobs, $"GAGAL SEND DATA TASK [{kodeToko}][{judulTask}]", Utility.TipeLog.Debug);
                rtn = false;
            }
            finally
            {
                sqlBuild = null;
                sqlTask = null;
            }

            return rtn;
        }

        public async Task<bool> InsertTrPrPajakTask(string jobs, DataRow dr, Config conf, int idThreadDoWork)
        {
            bool rtn = false;
            string msgSvr = "";
            string sqlTask = "";
            string judulTask = "";
            var dateNow = DateTime.Now;
            byte[] tempByte = null;

            try
            {
                string V(string col) => dr[col].ToString().Replace("'", "''").Replace("\\", "\\\\");

                sqlTask = "DROP TABLE IF EXISTS `pos`.`temp_pajak`; " +
                          "CREATE TABLE IF NOT EXISTS `pos`.`temp_pajak` (" +
                          " `KD_DC` VARCHAR(4) NOT NULL," +
                          " `TOK_CODE` VARCHAR(4) NOT NULL," +
                          " `TOK_SKP` VARCHAR(100) NOT NULL," +
                          " `TOK_NAMA_FRC` VARCHAR(100) NOT NULL," +
                          " `TOK_NPWP` VARCHAR(100) NOT NULL," +
                          " `START_TGL_BERLAKU_SE` VARCHAR(100) NOT NULL," +
                          " `END_TGL_BERLAKU_SE` VARCHAR(100) NOT NULL," +
                          " `FLAG_PKP` VARCHAR(100) NOT NULL," +
                          " `UPDID_TOKO` VARCHAR(45) DEFAULT NULL," +
                          " `UPDTIME_TOKO` DATETIME DEFAULT NULL," +
                          " PRIMARY KEY (`KD_DC`,`TOK_CODE`)," +
                          " KEY `IDX_DC` (`KD_DC`)," +
                          " KEY `IDX_TOKO` (`TOK_CODE`)" +
                          ") ENGINE=INNODB DEFAULT CHARSET=latin1;" +
                          "\nINSERT IGNORE INTO `pos`.`temp_pajak` (" +
                          "`KD_DC`,`TOK_CODE`,`TOK_SKP`," +
                          "`TOK_NAMA_FRC`,`TOK_NPWP`,`START_TGL_BERLAKU_SE`," +
                          "`END_TGL_BERLAKU_SE`,`FLAG_PKP`," +
                          "`UPDID_TOKO`,`UPDTIME_TOKO`) VALUES " +
                          $"\n('{V("KD_DC")}','{V("TOK_CODE")}','{V("TOK_SKP")}'," +
                          $" '{V("TOK_NAMA_FRC")}','{V("TOK_NPWP")}','{V("START_TGL_BERLAKU_SE")}'," +
                          $" '{V("END_TGL_BERLAKU_SE")}','{V("FLAG_PKP")}'," +
                          $" USER(), NOW());";

                judulTask = $"PAJAK|{dr["TOK_CODE"].ToString().ToUpper()}|{dateNow:yyyyMMddHHmm}";

                // Revisi (27 Oktober 2023) : tambah cek opsi pengiriman data ke server POSRT cabang
                if (conf.IsPosRT_RestApi)
                {
                    if (! await _objSvr.SendTrPrTask_RestApi(jobs, sqlTask, dr["TOK_CODE"].ToString().ToUpper(), judulTask, conf, msgSvr))
                    {
                        sqlTask = null;
                        goto ExitTry;
                    }
                }
                else
                {
                    tempByte = _objComp.Compress(sqlTask);
                    if (! await _objSvr.SendTrPrTask_Service(jobs, tempByte, dr["TOK_CODE"].ToString().ToUpper(), judulTask, conf, msg => msgSvr = msg))
                    {
                        sqlTask = null;
                        goto ExitTry;
                    }
                }

                _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|SUKSES SEND DATA TASK [{dr["TOK_CODE"]}][{judulTask}]", Utility.TipeLog.Debug);
                rtn = true;

            ExitTry:;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|GAGAL SEND DATA TASK [{dr["TOK_CODE"]}][{judulTask}]", Utility.TipeLog.Debug);
                rtn = false;
            }
            finally
            {
                sqlTask = null;
            }

            return rtn;
        }

        public async Task<bool> InsertSaranaProdukKhusus_Task(string jobs, string kodeToko, string tglStatus, string tglProses, Config conf, int idThreadDoWork)
        {
            bool rtn = false;
            string msgSvr = "";
            var sqlBuild = new StringBuilder();
            string judulTask = "";
            var dateNow = DateTime.Now;
            byte[] tempByte = null;

            try
            {
                using var dtService = db.CreateConnection();

                var dtSaranaProduk = (await dtService.QueryAsync<dynamic>(
                    $"SELECT `TOK_CODE`,`NAMA_MEREK`,`NAMA_SARANA`,`TGL_AKTIF_MEREK`,`TGL_NONAKTIF_MEREK`" +
                    $" FROM `{db.DatabaseName()}`.`master_sarana_produk_khusus`" +
                    $" WHERE `TOK_CODE` = '{kodeToko}' AND `TGL_STATUS` = '{tglStatus}';")).ToList();

                if (dtSaranaProduk.Count > 0)
                {
                    // Insert Data Master Sarana Produk Khusus Toko ke Tabel Task PosRealTime Cabang
                    sqlBuild.Append("CREATE TABLE IF NOT EXISTS `pos`.`temp_sarana_produk_khusus` (" +
                                    " `TOK_CODE` VARCHAR(25) DEFAULT NULL," +
                                    " `NAMA_MEREK` VARCHAR(100) DEFAULT NULL," +
                                    " `NAMA_SARANA` VARCHAR(100) DEFAULT NULL," +
                                    " `TGL_AKTIF_MEREK` VARCHAR(25) DEFAULT NULL," +
                                    " `TGL_NONAKTIF_MEREK` VARCHAR(25) DEFAULT NULL," +
                                    " `ADDID` VARCHAR(50) DEFAULT NULL," +
                                    " `ADDTIME` DATETIME DEFAULT NULL," +
                                    " KEY `IDX_TOK_CODE` (`TOK_CODE`)" +
                                    ") ENGINE=INNODB DEFAULT CHARSET=latin1;" +
                                    "TRUNCATE `pos`.`temp_sarana_produk_khusus`;");

                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_sarana_produk_khusus` (" +
                                    "`TOK_CODE`,`NAMA_MEREK`,`NAMA_SARANA`," +
                                    "`TGL_AKTIF_MEREK`,`TGL_NONAKTIF_MEREK`," +
                                    "`ADDID`,`ADDTIME`) VALUES ");

                    foreach (var dr in dtSaranaProduk)
                    {
                        string tokCode = ((string)dr.TOK_CODE).Replace("'", "''").Replace("\\", "\\\\");
                        string namaMerek = ((string)dr.NAMA_MEREK).Replace("'", "''").Replace("\\", "\\\\");
                        string namaSarana = ((string)dr.NAMA_SARANA).Replace("'", "''").Replace("\\", "\\\\");
                        string tglAktif = ((string)dr.TGL_AKTIF_MEREK).Replace("'", "''").Replace("\\", "\\\\");
                        string tglNonaktif = ((string)dr.TGL_NONAKTIF_MEREK).Replace("'", "''").Replace("\\", "\\\\");

                        sqlBuild.Append($"('{tokCode}','{namaMerek}','{namaSarana}'," +
                                        $" '{tglAktif}','{tglNonaktif}'," +
                                        $" USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sqlBuild.Append(";");

                    judulTask = $"SARPRODKHUS|{kodeToko.ToUpper()}|{dateNow:yyyyMMddHHmm}";

                    // Revisi (27 Oktober 2023) : tambah cek opsi pengiriman data ke server POSRT cabang
                    if (conf.IsPosRT_RestApi)
                    {
                        if (! await _objSvr.SendTrPrTask_RestApi(jobs, sqlBuild.ToString(), kodeToko.ToUpper(), judulTask, conf, msgSvr))
                            goto ExitTry;
                    }
                    else
                    {
                        tempByte = _objComp.Compress(sqlBuild.ToString());
                        if (!await _objSvr.SendTrPrTask_Service(jobs, tempByte, kodeToko.ToUpper(), judulTask, conf, msg => msgSvr = msg))
                            goto ExitTry;
                    }

                    _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|SUKSES SEND DATA TASK [{kodeToko.ToUpper()}][{judulTask}]", Utility.TipeLog.Debug);
                }

                rtn = true;
            ExitTry:;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                rtn = false;
            }
            finally
            {
                sqlBuild.Clear();
            }

            return rtn;
        }

        public async Task<bool> SendProdukKhusus_PersediaanToko(string jobs, string kodeToko, string tglStatus, string tglProses, Config conf, int idThreadDoWork)
        {
            bool rtn = false;
            string msgSvr = "";
            var sqlBuild = new StringBuilder();

            try
            {
                using var dtService = db.CreateConnection();

                // Revisi (01 Juli 2024) : Tambah kolom RASIO_BELI, SALES_GROWTH, KELIPATAN_PRODUKSI - Memo 428/CPS/24 & PRPK 548/04-24/E/PMO
                var dtProdKhusus = (await dtService.QueryAsync<dynamic>(
                    $"SELECT `KODE_MODUL`,`NAMA_MODUL`,`FLAG_SO`,`JADWAL_HARI_SO`,`PLU`," +
                    $" `RASIO_BELI`,`SALES_GROWTH`,`KELIPATAN_PRODUKSI`" +
                    $" FROM `{db.DatabaseName()}`.`master_produk_khusus`" +
                    $" WHERE DATE(`TGL_STATUS`) = '{tglStatus}';")).ToList();

                if (dtProdKhusus.Count > 0)
                {
                    // Revisi (01 Juli 2024) : Tambah kolom RASIO_BELI, SALES_GROWTH, KELIPATAN_PRODUKSI
                    sqlBuild.Append("CREATE TABLE IF NOT EXISTS `pos`.`temp_produk_khusus` (" +
                                    " `KODE_MODUL` BIGINT(20) NOT NULL," +
                                    " `NAMA_MODUL` varchar(100) DEFAULT NULL," +
                                    " `FLAG_SO` VARCHAR(1) DEFAULT NULL," +
                                    " `JADWAL_HARI_SO` VARCHAR(10) DEFAULT NULL," +
                                    " `PLU` VARCHAR(8) NOT NULL," +
                                    " `RASIO_BELI` VARCHAR(20) DEFAULT NULL," +
                                    " `SALES_GROWTH` VARCHAR(20) DEFAULT NULL," +
                                    " `KELIPATAN_PRODUKSI` VARCHAR(20) DEFAULT NULL," +
                                    " `ADDID` VARCHAR(50) DEFAULT NULL," +
                                    " `ADDTIME` DATETIME DEFAULT NULL," +
                                    " PRIMARY KEY (`KODE_MODUL`,`PLU`)" +
                                    ") ENGINE=INNODB DEFAULT CHARSET=latin1;");

                    // Sql alter tambah kolom RASIO_BELI
                    sqlBuild.Append(" SELECT COUNT(*) INTO @exist FROM information_schema.columns" +
                                    " WHERE table_schema = 'pos' AND COLUMN_NAME = 'RASIO_BELI' AND table_name = 'temp_produk_khusus' LIMIT 1;" +
                                    " SET @query = IF(@exist <= 0, 'ALTER TABLE `pos`.`temp_produk_khusus` ADD COLUMN `RASIO_BELI` VARCHAR(20) DEFAULT NULL', 'SELECT ''OK''');" +
                                    " PREPARE stmt FROM @query; EXECUTE stmt;");
                    // Sql alter tambah kolom SALES_GROWTH
                    sqlBuild.Append(" SELECT COUNT(*) INTO @exist FROM information_schema.columns" +
                                    " WHERE table_schema = 'pos' AND COLUMN_NAME = 'SALES_GROWTH' AND table_name = 'temp_produk_khusus' LIMIT 1;" +
                                    " SET @query = IF(@exist <= 0, 'ALTER TABLE `pos`.`temp_produk_khusus` ADD COLUMN `SALES_GROWTH` VARCHAR(20) DEFAULT NULL', 'SELECT ''OK''');" +
                                    " PREPARE stmt FROM @query; EXECUTE stmt;");
                    // Sql alter tambah kolom KELIPATAN_PRODUKSI
                    sqlBuild.Append(" SELECT COUNT(*) INTO @exist FROM information_schema.columns" +
                                    " WHERE table_schema = 'pos' AND COLUMN_NAME = 'KELIPATAN_PRODUKSI' AND table_name = 'temp_produk_khusus' LIMIT 1;" +
                                    " SET @query = IF(@exist <= 0, 'ALTER TABLE `pos`.`temp_produk_khusus` ADD COLUMN `KELIPATAN_PRODUKSI` VARCHAR(20) DEFAULT NULL', 'SELECT ''OK''');" +
                                    " PREPARE stmt FROM @query; EXECUTE stmt;");

                    sqlBuild.Append(" TRUNCATE `pos`.`temp_produk_khusus`; ");
                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_produk_khusus` (" +
                                    "`KODE_MODUL`,`NAMA_MODUL`,`FLAG_SO`," +
                                    "`JADWAL_HARI_SO`,`PLU`," +
                                    "`RASIO_BELI`,`SALES_GROWTH`,`KELIPATAN_PRODUKSI`," +
                                    "`ADDID`,`ADDTIME`) VALUES ");

                    foreach (var dr in dtProdKhusus)
                    {
                        string V(string val) => val.Replace("'", "''").Replace("\\", "\\\\");
                        sqlBuild.Append($"('{V(dr.KODE_MODUL.ToString())}','{V(dr.NAMA_MODUL.ToString())}','{V(dr.FLAG_SO.ToString())}'," +
                                        $" '{V(dr.JADWAL_HARI_SO.ToString())}','{V(dr.PLU.ToString())}'," +
                                        $" '{V(dr.RASIO_BELI.ToString())}','{V(dr.SALES_GROWTH.ToString())}','{V(dr.KELIPATAN_PRODUKSI.ToString())}'," +
                                        $" USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sqlBuild.Append(";");

                    if (kodeToko == "ALL") kodeToko = "ALL_STORE";

                    if (await _objSvr.InsertTaskGzip_PersediaanToko(jobs, kodeToko, "PRODUK_KHUSUS", "text/sql", sqlBuild.ToString(), conf, msg => msgSvr = msg))
                    {
                        _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|SUKSES SEND DATA PERSEDIAAN TOKO [ALL_STORE][PRODUK_KHUSUS]", Utility.TipeLog.Debug);
                        rtn = true;
                    }
                    else
                    {
                        _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|GAGAL SEND DATA PERSEDIAAN TOKO [ALL_STORE][PRODUK_KHUSUS]\nMESSAGE : {msgSvr}", Utility.TipeLog.Debug);
                        rtn = false;
                    }
                }
                else
                {
                    _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|GAGAL SEND DATA PERSEDIAAN TOKO [ALL_STORE][PRODUK_KHUSUS]\n" +
                                            $"DtProdKhusus.Count : {dtProdKhusus.Count}\nTglStatus : {tglStatus}\nTglProses : {tglProses}", Utility.TipeLog.Debug);
                    rtn = false;
                }
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                rtn = false;
            }
            finally
            {
                sqlBuild.Clear();
            }

            return rtn;
        }

        public async Task<bool> SendTAT_PersediaanToko(string jobs, string kodeToko, string tglStatus, string tglProses, Config conf, int idThreadDoWork)
        {
            bool rtn = false;
            string msgSvr = "";
            var sqlBuild = new StringBuilder();

            try
            {
                using var dtService = db.CreateConnection();

                // Ambil Data Master TAT Toko
                var dtTAT = (await dtService.QueryAsync<dynamic>(
                    $"SELECT `TAT_ID`,`KD_PLU`,`TGL_AWAL`,`TGL_AKHIR`,`FLAG_PROD`" +
                    $" FROM `{db.DatabaseName()}`.`master_tat`" +
                    $" WHERE DATE(`TGL_STATUS`) = '{tglStatus}';")).ToList();

                if (dtTAT.Count > 0)
                {
                    sqlBuild.Append("CREATE TABLE IF NOT EXISTS `pos`.`temp_tat_ws` (" +
                                    " `TAT_ID` VARCHAR(50) NOT NULL DEFAULT ''," +
                                    " `KD_PLU` VARCHAR(10) NOT NULL DEFAULT ''," +
                                    " `TGL_AWAL` VARCHAR(20) NOT NULL DEFAULT ''," +
                                    " `TGL_AKHIR` VARCHAR(20) NOT NULL DEFAULT ''," +
                                    " `FLAG_PROD` VARCHAR(200) DEFAULT ''," +
                                    " `ADDID` VARCHAR(50) DEFAULT NULL," +
                                    " `ADDTIME` DATETIME DEFAULT NULL," +
                                    " PRIMARY KEY (`TAT_ID`,`KD_PLU`,`TGL_AWAL`,`TGL_AKHIR`)" +
                                    ") ENGINE=INNODB DEFAULT CHARSET=latin1;" +
                                    "TRUNCATE `pos`.`temp_tat_ws`;");

                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_tat_ws` (" +
                                    "`TAT_ID`,`KD_PLU`," +
                                    "`TGL_AWAL`,`TGL_AKHIR`,`FLAG_PROD`," +
                                    "`ADDID`,`ADDTIME`) VALUES ");

                    foreach (var dr in dtTAT)
                    {
                        string V(string val) => val.Replace("'", "''").Replace("\\", "\\\\");
                        sqlBuild.Append($"('{V(dr.TAT_ID.ToString())}','{V(dr.KD_PLU.ToString())}'," +
                                        $" '{V(dr.TGL_AWAL.ToString())}','{V(dr.TGL_AKHIR.ToString())}','{V(dr.FLAG_PROD.ToString())}'," +
                                        $" USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sqlBuild.Append(";");

                    if (kodeToko == "ALL") kodeToko = "ALL_STORE";

                    if (await _objSvr.InsertTaskGzip_PersediaanToko(jobs, kodeToko, "TAT", "text/sql", sqlBuild.ToString(), conf, msg => msgSvr = msg))
                    {
                        _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|SUKSES SEND DATA PERSEDIAAN TOKO [ALL_STORE][TAT]", Utility.TipeLog.Debug);
                        rtn = true;
                    }
                    else
                    {
                        _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|GAGAL SEND DATA PERSEDIAAN TOKO [ALL_STORE][TAT]\nMESSAGE : {msgSvr}", Utility.TipeLog.Debug);
                        rtn = false;
                    }
                }
                else
                {
                    _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|GAGAL SEND DATA PERSEDIAAN TOKO [ALL_STORE][TAT]\n" +
                                            $"DtTAT.Count : {dtTAT.Count}\nTglStatus : {tglStatus}\nTglProses : {tglProses}", Utility.TipeLog.Debug);
                    rtn = false;
                }
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                rtn = false;
            }
            finally
            {
                sqlBuild.Clear();
            }

            return rtn;
        }

        public async Task<bool> SendTTT_PersediaanToko(string jobs, string kodeToko, string tglStatus, string tglProses, Config conf, int idThreadDoWork)
        {
            bool rtn = false;
            string msgSvr = "";
            var sqlBuild = new StringBuilder();

            try
            {
                using var dtService = db.CreateConnection();

                // Ambil Data Master TTT Toko
                var dtTTT = (await dtService.QueryAsync<dynamic>(
                    $"SELECT `TAT_ID`,`KD_TOKO`,`KD_TOKO_PENERIMA`" +
                    $" FROM `{db.DatabaseName()}`.`master_ttt`" +
                    $" WHERE DATE(`TGL_STATUS`) = '{tglStatus}';")).ToList();

                if (dtTTT.Count > 0)
                {
                    sqlBuild.Append("CREATE TABLE IF NOT EXISTS `pos`.`temp_ttt_ws` (" +
                                    " `TAT_ID` VARCHAR(50) NOT NULL DEFAULT ''," +
                                    " `KD_TOKO` VARCHAR(5) NOT NULL DEFAULT ''," +
                                    " `KD_TOKO_PENERIMA` varchar(5) NOT NULL DEFAULT ''," +
                                    " `ADDID` VARCHAR(50) DEFAULT NULL," +
                                    " `ADDTIME` DATETIME DEFAULT NULL," +
                                    " PRIMARY KEY (`TAT_ID`,`KD_TOKO`,`KD_TOKO_PENERIMA`)" +
                                    ") ENGINE=INNODB DEFAULT CHARSET=latin1;" +
                                    "TRUNCATE `pos`.`temp_ttt_ws`;");

                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_ttt_ws` (" +
                                    "`TAT_ID`,`KD_TOKO`,`KD_TOKO_PENERIMA`," +
                                    "`ADDID`,`ADDTIME`) VALUES ");

                    foreach (var dr in dtTTT)
                    {
                        string V(string val) => val.Replace("'", "''").Replace("\\", "\\\\");
                        sqlBuild.Append($"('{V(dr.TAT_ID.ToString())}','{V(dr.KD_TOKO.ToString())}','{V(dr.KD_TOKO_PENERIMA.ToString())}'," +
                                        $" USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sqlBuild.Append(";");

                    if (kodeToko == "ALL") kodeToko = "ALL_STORE";

                    if (await _objSvr.InsertTaskGzip_PersediaanToko(jobs, kodeToko, "TTT", "text/sql", sqlBuild.ToString(), conf, msg => msgSvr = msg))
                    {   
                        _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|SUKSES SEND DATA PERSEDIAAN TOKO [ALL_STORE][TTT]", Utility.TipeLog.Debug);
                        rtn = true;
                    }
                    else
                    {
                        _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|GAGAL SEND DATA PERSEDIAAN TOKO [ALL_STORE][TTT]\nMESSAGE : {msgSvr}", Utility.TipeLog.Debug);
                        rtn = false;
                    }
                }
                else
                {
                    _objUtil.Tracelog(jobs, $"THREAD {idThreadDoWork}|GAGAL SEND DATA PERSEDIAAN TOKO [ALL_STORE][TTT]\n" +
                                            $"DtTTT.Count : {dtTTT.Count}\nTglStatus : {tglStatus}\nTglProses : {tglProses}", Utility.TipeLog.Debug);
                    rtn = false;
                }
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                rtn = false;
            }
            finally
            {
                sqlBuild.Clear();
            }

            return rtn;
        }
    }
}
