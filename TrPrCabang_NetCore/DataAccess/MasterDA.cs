using Dapper;
using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;
using TrPrCabang_NetCore.Connection;
using TrPrCabang_NetCore.Models;

namespace TrPrCabang_NetCore.DataAccess
{
    public class MasterDA
    {
        private readonly Utility _objUtil;
        private readonly IDbServices db;

        public MasterDA(IDbServices dbService)
        {
            db = dbService;
        }
        public async Task<bool> InsertMasterTKX(string jobs, List<TKX> objTKX, DateTime tglStatus, DateTime tglProses)
        {
            using var dtService = db.CreateConnection();
            int countIns = 50;
            int iCount = 0;
            string sql = "";

            try
            {
                // Cek table master all TKX ada atau tidak
                // Revisi (17 Juli 2023) : Ubah tipe data kolom FLAGTK dari VARCHAR[200] menjadi TEXT
                // Revisi (18 Desember 2024) : Tambah data kolom NITKU

                sql = $" " +
                      $"CREATE TABLE IF NOT EXISTS ` " +
                      $" {db.DatabaseName()}  " +
                      $"`.`master_all_tkx` (`RECID1` VARCHAR(25) DEFAULT '',`RECID` VARCHAR(25) DEFAULT '', " +
                      $" `KDSTRATA` VARCHAR(25) DEFAULT '',`KDORGAN` VARCHAR(25) DEFAULT '',`KDLOC` VARCHAR(50) DEFAULT '',`KDTK` VARCHAR(25) NOT NULL, " +
                      $" `NAMA` VARCHAR(200) DEFAULT '',`ALMT` VARCHAR(500) DEFAULT '',`KOTA` VARCHAR(100) DEFAULT '',`RT` VARCHAR(25) DEFAULT '',`RW` VARCHAR(25) DEFAULT '', " +
                      $" `TELP` VARCHAR(25) DEFAULT '',`TELP_1` VARCHAR(25) DEFAULT '',`TELP_2` VARCHAR(25) DEFAULT '',`TELP_3` VARCHAR(25) DEFAULT '',`FAX_1` VARCHAR(25) DEFAULT '', " +
                      $" `FAX_2` VARCHAR(25) DEFAULT '',`FAX_3` VARCHAR(25) DEFAULT '',`KODE_POS` VARCHAR(25) DEFAULT '',`GATE` VARCHAR(25) DEFAULT '',`COMP` VARCHAR(25) DEFAULT '', " +
                      $" `LUAS` VARCHAR(25) DEFAULT '',`BUKA` VARCHAR(25) DEFAULT '',`MODE` VARCHAR(25) DEFAULT '',`TYPE_TOKO` VARCHAR(25) DEFAULT '',`FOT` VARCHAR(25) DEFAULT '', " +
                      $" `TYPE_HARGA` VARCHAR(25) DEFAULT '',`TYPE_RAK` VARCHAR(25) DEFAULT '',`LS_TANAH` VARCHAR(25) DEFAULT '',`LS_JUAL` VARCHAR(25) DEFAULT '', " +
                      $" `LS_DISP` VARCHAR(25) DEFAULT '',`LS_GUDANG` VARCHAR(25) DEFAULT '',`PKP` VARCHAR(25) DEFAULT '',`NPWP` VARCHAR(50) DEFAULT '',`SKP` VARCHAR(50) DEFAULT '', " +
                      $" `TGLSKP` VARCHAR(25) DEFAULT '',`TUTUP` VARCHAR(25) DEFAULT '',`KIRIM` VARCHAR(25) DEFAULT '',`GUDANG` VARCHAR(25) DEFAULT '',`KONT_AWAL` VARCHAR(25) DEFAULT '', " +
                      $" `KONT_AKHIR` VARCHAR(25) DEFAULT '',`CNT_HAL` VARCHAR(25) DEFAULT '',`CNT_LT` VARCHAR(25) DEFAULT '',`COC` VARCHAR(25) DEFAULT '',`COC_TIANG` VARCHAR(25) DEFAULT '', " +
                      $" `END_GOND` VARCHAR(25) DEFAULT '',`JMLRAK` VARCHAR(25) DEFAULT '',`FLDISP` VARCHAR(25) DEFAULT '',`FREEGDL` VARCHAR(25) DEFAULT '',`PRPLPRM` VARCHAR(25) DEFAULT '', " +
                      $" `PRMAREA` VARCHAR(25) DEFAULT '',`SEWA` VARCHAR(25) DEFAULT '',`KURS` VARCHAR(25) DEFAULT '',`UMUR` VARCHAR(25) DEFAULT '',`NAMAFRC` VARCHAR(100) DEFAULT '', " +
                      $" `MARKUP` VARCHAR(25) DEFAULT '',`KPP` VARCHAR(25) DEFAULT '',`PERSONIL` VARCHAR(25) DEFAULT '',`KA_TOKO` VARCHAR(25) DEFAULT '', " +
                      $" `AS_TOKO` VARCHAR(25) DEFAULT '',`MERCHAN01` VARCHAR(25) DEFAULT '',`MERCHAN02` VARCHAR(25) DEFAULT '',`MERCHAN03` VARCHAR(25) DEFAULT '', " +
                      $" `MERCHAN04` VARCHAR(25) DEFAULT '',`KASIR01` VARCHAR(25) DEFAULT '',`KASIR02` VARCHAR(25) DEFAULT '',`KASIR03` VARCHAR(25) DEFAULT '', " +
                      $" `KASIR04` VARCHAR(25) DEFAULT '',`KASIR05` VARCHAR(25) DEFAULT '',`KASIR06` VARCHAR(25) DEFAULT '',`PRAMU01` VARCHAR(25) DEFAULT '',`PRAMU02` VARCHAR(25) DEFAULT '', " +
                      $" `PRAMU03` VARCHAR(25) DEFAULT '',`PRAMU04` VARCHAR(25) DEFAULT '',`PRAMU05` VARCHAR(25) DEFAULT '',`PRAMU06` VARCHAR(25) DEFAULT '',`PRAMU07` VARCHAR(25) DEFAULT '', " +
                      $" `PRAMU08` VARCHAR(25) DEFAULT '',`PRAMU09` VARCHAR(25) DEFAULT '',`PRAMU10` VARCHAR(25) DEFAULT '',`PRAMU11` VARCHAR(25) DEFAULT '',`PRAMU12` VARCHAR(25) DEFAULT '', " +
                      $" `FR_MAX_CR` VARCHAR(25) DEFAULT '',`FR_BEGBAL` VARCHAR(25) DEFAULT '',`FR_CREDIT` VARCHAR(25) DEFAULT '',`FR_PPN` VARCHAR(25) DEFAULT '',`FR_PAY` VARCHAR(25) DEFAULT '', " +
                      $" `FR_PPN1` VARCHAR(25) DEFAULT '',`FR_D_CR` VARCHAR(25) DEFAULT '',`FR_D_PAY` VARCHAR(25) DEFAULT '',`UPDATE` VARCHAR(25) DEFAULT '',`TOKO` VARCHAR(25) DEFAULT '', " +
                      $" `NPB` VARCHAR(25) DEFAULT '',`BPB` VARCHAR(25) DEFAULT '',`BADAN` VARCHAR(25) DEFAULT '',`ROYAL_1` VARCHAR(25) DEFAULT '',`ROYAL_1P` VARCHAR(25) DEFAULT '', " +
                      $" `ROYAL_2` VARCHAR(25) DEFAULT '',`ROYAL_2P` VARCHAR(25) DEFAULT '',`ROYAL_3` VARCHAR(25) DEFAULT '',`ROYAL_3P` VARCHAR(25) DEFAULT '',`ROYAL_4` VARCHAR(25) DEFAULT '', " +
                      $" `ROYAL_4P` VARCHAR(25) DEFAULT '',`BUAH` VARCHAR(25) DEFAULT '',`TGL_PKM` VARCHAR(25) DEFAULT '',`JML_KM` VARCHAR(25) DEFAULT '',`ELPIJI` VARCHAR(25) DEFAULT '', " +
                      $" `KDLM` VARCHAR(25) DEFAULT '',`TRF` VARCHAR(25) DEFAULT '',`UPD_DHO` VARCHAR(25) DEFAULT '',`UPD_DEC` VARCHAR(25) DEFAULT '',`UPD_AAC` VARCHAR(25) DEFAULT '', " +
                      $" `FDISP` VARCHAR(25) DEFAULT '',`COMPETE` VARCHAR(50) DEFAULT '',`TRF_HO` VARCHAR(25) DEFAULT '',`OTO` VARCHAR(25) DEFAULT '',`KDBR` VARCHAR(25) DEFAULT '', " +
                      $" `BERLAKU` VARCHAR(25) DEFAULT '',`NPWP_DC` VARCHAR(25) DEFAULT '',`TGL_DC` VARCHAR(25) DEFAULT '',`ALMT_PJK1` VARCHAR(500) DEFAULT '',`ALMT_PJK2` VARCHAR(500) DEFAULT '', " +
                      $" `ALMT_PJK3` VARCHAR(500) DEFAULT '',`KODE_CID` VARCHAR(25) DEFAULT '',`KD_FRC` VARCHAR(25) DEFAULT '',`NPPKP` VARCHAR(25) DEFAULT '',`MRCHNT` VARCHAR(25) DEFAULT '', " +
                      $" `TOK24` VARCHAR(25) DEFAULT '', " +
                      $" `CONV` VARCHAR(25) DEFAULT '',`BERLAKU_HRG` VARCHAR(25) DEFAULT '',`BERLAKU_CID` VARCHAR(25) DEFAULT '',`JADWAL_CONV` VARCHAR(25) DEFAULT '', " +
                      $" `FLAGTOKO` VARCHAR(50) DEFAULT '',`HARI24` VARCHAR(25) DEFAULT '',`IMOBILE` VARCHAR(25) DEFAULT '',`APKA` VARCHAR(25) DEFAULT '',`TIDAK_HITUNG_PB` VARCHAR(25) DEFAULT '', " +
                      $" `JENIS_TOKO` VARCHAR(25) DEFAULT '',`CLASS_TK` VARCHAR(25) DEFAULT '',`SUBCLASS_TK` VARCHAR(25) DEFAULT '',`TGL_AKTIF_MSTORE` VARCHAR(25) DEFAULT '', " +
                      $" `KONEKSI_PRIMARY` VARCHAR(25) DEFAULT '',`KONEKSI_SECONDARY` VARCHAR(25) DEFAULT '',`TANGGAL_SECONDARY` VARCHAR(25) DEFAULT '',`TANGGAL_PRIMARY` VARCHAR(25) DEFAULT '', " +
                      $" `DOMAIN` VARCHAR(25) DEFAULT '', " +
                      $" `DUAL_SCREEN` VARCHAR(25) DEFAULT '',`START_TGL_BERLAKU_SE` VARCHAR(25) DEFAULT '',`END_TGL_BERLAKU_SE` VARCHAR(25) DEFAULT '', " +
                      $" `FLAG_LIBUR_RUTIN` VARCHAR(25) DEFAULT '',`JAM_BUKA` VARCHAR(25) DEFAULT '',`JAM_TUTUP` VARCHAR(25) DEFAULT '',`POINT_CAFE` VARCHAR(5) DEFAULT '', " +
                      $" `EVENT` VARCHAR(25) DEFAULT '',`PERIODE_AWAL` VARCHAR(25) DEFAULT '',`PERIODE_AKHIR` VARCHAR(25) DEFAULT '', " +
                      $" `TGL_BUKA_POINTCAFE` VARCHAR(25) DEFAULT '',`TGL_TUTUP_POINTCAFE` VARCHAR(25) DEFAULT '',`TGL_DIS05` VARCHAR(25) DEFAULT '',`KABUPATEN` VARCHAR(50) DEFAULT '', " +
                      $" `SETOR_BANK` VARCHAR(25) DEFAULT '',`NAMA_BANK1` VARCHAR(100) DEFAULT '',`TGL_AWAL_BANK1` VARCHAR(25) DEFAULT '',`TGL_AKHIR_BANK1` VARCHAR(25) DEFAULT '', " +
                      $" `NAMA_BANK2` VARCHAR(100) DEFAULT '',`TGL_AWAL_BANK2` VARCHAR(25) DEFAULT '',`TGL_AKHIR_BANK2` VARCHAR(25) DEFAULT '',`TOK_PERDA` VARCHAR(25) DEFAULT '', " +
                      $" `TOK_TGL_AWAL_PERDA` VARCHAR(25) DEFAULT '',`TOK_TGL_AKHIR_PERDA` VARCHAR(25) DEFAULT '',`TOK_POINTCAFFEE_APP` VARCHAR(25) DEFAULT '',`TOK_DRIVE_THRU` VARCHAR(25) DEFAULT '', " +
                      $" `ALMT_TOKO` VARCHAR(500) DEFAULT '',`ALMT_PAJAK` VARCHAR(500) DEFAULT '',`TOK_PT_BIP` VARCHAR(25) DEFAULT '',`FLAGTK` TEXT, " +
                      $" `MARKUP_IGR` VARCHAR(300) DEFAULT '',`TGL_MULTIRATES` VARCHAR(50) DEFAULT '',`NITKU` VARCHAR(50) DEFAULT '', " +
                      $" `TGL_STATUS` DATETIME DEFAULT NULL, `TGL_PROSES` DATETIME DEFAULT NULL,`UPDID_CABANG` VARCHAR(100) DEFAULT NULL,`UPDTIME_CABANG` DATETIME DEFAULT NULL, " +
                      $" PRIMARY KEY (`KDTK`), KEY `IDX_TGL_STATUS` (`TGL_STATUS`), KEY `IDX_TGL_PROSES` (`TGL_PROSES`)) ENGINE=INNODB DEFAULT CHARSET=latin1; " +
                      $"";
                await dtService.ExecuteScalarAsync(sql);

                // Revisi (17 Juli 2023) : Ubah tipe data kolom FLAGTK dari VARCHAR[200] menjadi TEXT
                sql = $"SELECT COUNT(DATA_TYPE) FROM INFORMATION_SCHEMA.COLUMNS" +
                     $" WHERE TABLE_SCHEMA = '{db.DatabaseName()}' AND TABLE_NAME = 'master_all_tkx'" +
                     $" AND COLUMN_NAME = 'FLAGTK' AND DATA_TYPE = 'text';";
                int columnCount = await dtService.ExecuteScalarAsync<int>(sql);
                if (columnCount == 0)
                {
                    sql = $"ALTER TABLE `{db.DatabaseName()}`.`master_all_tkx` MODIFY COLUMN `FLAGTK` TEXT;";
                    await dtService.ExecuteScalarAsync(sql);
                }

                // Revisi (17 Juli 2023) : Alter tambah data kolom NITKU

                sql = $"SELECT COUNT(DATA_TYPE) FROM INFORMATION_SCHEMA.COLUMNS" +
                       $" WHERE TABLE_SCHEMA = '{db.DatabaseName()}' AND TABLE_NAME = 'master_all_tkx'" +
                       $" AND COLUMN_NAME = 'NITKU';";
                int columnNITKU = await dtService.ExecuteScalarAsync<int>(sql);
                if (columnNITKU == 0)
                {
                    sql = $"ALTER TABLE `{db.DatabaseName()}`.`master_all_tkx` ADD COLUMN `NITKU` VARCHAR(50) DEFAULT '' AFTER `TGL_MULTIRATES`;";
                    await dtService.ExecuteScalarAsync(sql);
                }

                // Truncate table master all TKX
                sql = $"TRUNCATE TABLE `{db.DatabaseName()}`.`master_all_tkx`;";
                await dtService.ExecuteScalarAsync(sql);

                if (objTKX.Count > 0)
                {
                    const string columns = @"(`RECID1`,`RECID`,`KDSTRATA`,`KDORGAN`,`KDLOC`,`KDTK`,`NAMA`,`ALMT`,`KOTA`,
                                           `RT`,`RW`,`TELP`,`TELP_1`,`TELP_2`,`TELP_3`,`FAX_1`,`FAX_2`,`FAX_3`,`KODE_POS`,`GATE`,
                                           `COMP`,`LUAS`,`BUKA`,`MODE`,`TYPE_TOKO`,`FOT`,`TYPE_HARGA`,`TYPE_RAK`,`LS_TANAH`,`LS_JUAL`,
                                           `LS_DISP`,`LS_GUDANG`,`PKP`,`NPWP`,`SKP`,`TGLSKP`,`TUTUP`,`KIRIM`,`GUDANG`,`KONT_AWAL`,
                                           `KONT_AKHIR`,`CNT_HAL`,`CNT_LT`,`COC`,`COC_TIANG`,`END_GOND`,`JMLRAK`,`FLDISP`,`FREEGDL`,
                                           `PRPLPRM`,`PRMAREA`,`SEWA`,`KURS`,`UMUR`,`NAMAFRC`,`MARKUP`,`KPP`,`PERSONIL`,`KA_TOKO`,
                                           `AS_TOKO`,`MERCHAN01`,`MERCHAN02`,`MERCHAN03`,`MERCHAN04`,`KASIR01`,`KASIR02`,`KASIR03`,
                                           `KASIR04`,`KASIR05`,`KASIR06`,`PRAMU01`,`PRAMU02`,`PRAMU03`,`PRAMU04`,`PRAMU05`,`PRAMU06`,
                                           `PRAMU07`,`PRAMU08`,`PRAMU09`,`PRAMU10`,`PRAMU11`,`PRAMU12`,`FR_MAX_CR`,`FR_BEGBAL`,
                                           `FR_CREDIT`,`FR_PPN`,`FR_PAY`,`FR_PPN1`,`FR_D_CR`,`FR_D_PAY`,`UPDATE`,`TOKO`,`NPB`,`BPB`,
                                           `BADAN`,`ROYAL_1`,`ROYAL_1P`,`ROYAL_2`,`ROYAL_2P`,`ROYAL_3`,`ROYAL_3P`,`ROYAL_4`,`ROYAL_4P`,
                                           `BUAH`,`TGL_PKM`,`JML_KM`,`ELPIJI`,`KDLM`,`TRF`,`UPD_DHO`,`UPD_DEC`,`UPD_AAC`,`FDISP`,
                                           `COMPETE`,`TRF_HO`,`OTO`,`KDBR`,`BERLAKU`,`NPWP_DC`,`TGL_DC`,`ALMT_PJK1`,`ALMT_PJK2`,
                                           `ALMT_PJK3`,`KODE_CID`,`KD_FRC`,`NPPKP`,`MRCHNT`,`TOK24`,`CONV`,`BERLAKU_HRG`,`BERLAKU_CID`,
                                           `JADWAL_CONV`,`FLAGTOKO`,`HARI24`,`IMOBILE`,`APKA`,`TIDAK_HITUNG_PB`,`JENIS_TOKO`,`CLASS_TK`,
                                           `SUBCLASS_TK`,`TGL_AKTIF_MSTORE`,`KONEKSI_PRIMARY`,`KONEKSI_SECONDARY`,`TANGGAL_SECONDARY`,
                                           `TANGGAL_PRIMARY`,`DOMAIN`,`DUAL_SCREEN`,`START_TGL_BERLAKU_SE`,`END_TGL_BERLAKU_SE`,
                                           `FLAG_LIBUR_RUTIN`,`JAM_BUKA`,`JAM_TUTUP`,`POINT_CAFE`,`EVENT`,`PERIODE_AWAL`,`PERIODE_AKHIR`,
                                           `TGL_BUKA_POINTCAFE`,`TGL_TUTUP_POINTCAFE`,`TGL_DIS05`,`KABUPATEN`,`SETOR_BANK`,`NAMA_BANK1`,
                                           `TGL_AWAL_BANK1`,`TGL_AKHIR_BANK1`,`NAMA_BANK2`,`TGL_AWAL_BANK2`,`TGL_AKHIR_BANK2`,`TOK_PERDA`,
                                           `TOK_TGL_AWAL_PERDA`,`TOK_TGL_AKHIR_PERDA`,`TOK_POINTCAFFEE_APP`,`TOK_DRIVE_THRU`,`ALMT_TOKO`,
                                           `ALMT_PAJAK`,`TOK_PT_BIP`,`FLAGTK`,`MARKUP_IGR`,`TGL_MULTIRATES`,`NITKU`,
                                           `TGL_STATUS`,`TGL_PROSES`,`UPDID_CABANG`,`UPDTIME_CABANG`)";

                    string insertHeader = $"INSERT IGNORE INTO `{db.DatabaseName()}`.`master_all_tkx` {columns} VALUES ";
                    var sqlBuild = new StringBuilder(insertHeader);

                    foreach (var tkx in objTKX)
                    {
                        sqlBuild.Append($@"(
                                            '{tkx.RECID1.Replace("'", "''")}','{tkx.RECID.Replace("'", "''")}','{tkx.KDSTRATA.Replace("'", "''")}','{tkx.KDORGAN.Replace("'", "''")}',
                                            '{tkx.KDLOC.Replace("'", "''")}','{tkx.KDTK.Replace("'", "''")}','{tkx.NAMA.Replace("'", "''")}','{tkx.ALMT.Replace("'", "''")}',
                                            '{tkx.KOTA.Replace("'", "''")}','{tkx.RT.Replace("'", "''")}','{tkx.RW.Replace("'", "''")}','{tkx.TELP.Replace("'", "''")}',
                                            '{tkx.TELP_1.Replace("'", "''")}','{tkx.TELP_2.Replace("'", "''")}','{tkx.TELP_3.Replace("'", "''")}','{tkx.FAX_1.Replace("'", "''")}',
                                            '{tkx.FAX_2.Replace("'", "''")}','{tkx.FAX_3.Replace("'", "''")}','{tkx.KODE_POS.Replace("'", "''")}','{tkx.GATE.Replace("'", "''")}',
                                            '{tkx.COMP.Replace("'", "''")}','{tkx.LUAS.Replace("'", "''")}','{tkx.BUKA.Replace("'", "''")}','{tkx.MODE.Replace("'", "''")}',
                                            '{tkx.TYPE_TOKO.Replace("'", "''")}','{tkx.FOT.Replace("'", "''")}','{tkx.TYPE_HARGA.Replace("'", "''")}','{tkx.TYPE_RAK.Replace("'", "''")}',
                                            '{tkx.LS_TANAH.Replace("'", "''")}','{tkx.LS_JUAL.Replace("'", "''")}','{tkx.LS_DISP.Replace("'", "''")}','{tkx.LS_GUDANG.Replace("'", "''")}',
                                            '{tkx.PKP.Replace("'", "''")}','{tkx.NPWP.Replace("'", "''")}','{tkx.SKP.Replace("'", "''")}','{tkx.TGLSKP.Replace("'", "''")}',
                                            '{tkx.TUTUP.Replace("'", "''")}','{tkx.KIRIM.Replace("'", "''")}','{tkx.GUDANG.Replace("'", "''")}','{tkx.KONT_AWAL.Replace("'", "''")}',
                                            '{tkx.KONT_AKHIR.Replace("'", "''")}','{tkx.CNT_HAL.Replace("'", "''")}','{tkx.CNT_LT.Replace("'", "''")}','{tkx.COC.Replace("'", "''")}',
                                            '{tkx.COC_TIANG.Replace("'", "''")}','{tkx.END_GOND.Replace("'", "''")}','{tkx.JMLRAK.Replace("'", "''")}','{tkx.FLDISP.Replace("'", "''")}',
                                            '{tkx.FREEGDL.Replace("'", "''")}','{tkx.PRPLPRM.Replace("'", "''")}','{tkx.PRMAREA.Replace("'", "''")}','{tkx.SEWA.Replace("'", "''")}',
                                            '{tkx.KURS.Replace("'", "''")}','{tkx.UMUR.Replace("'", "''")}','{tkx.NAMAFRC.Replace("'", "''")}','{tkx.MARKUP.Replace("'", "''")}',
                                            '{tkx.KPP.Replace("'", "''")}','{tkx.PERSONIL.Replace("'", "''")}','{tkx.KA_TOKO.Replace("'", "''")}','{tkx.AS_TOKO.Replace("'", "''")}',
                                            '{tkx.MERCHAN01.Replace("'", "''")}','{tkx.MERCHAN02.Replace("'", "''")}','{tkx.MERCHAN03.Replace("'", "''")}','{tkx.MERCHAN04.Replace("'", "''")}',
                                            '{tkx.KASIR01.Replace("'", "''")}','{tkx.KASIR02.Replace("'", "''")}','{tkx.KASIR03.Replace("'", "''")}','{tkx.KASIR04.Replace("'", "''")}',
                                            '{tkx.KASIR05.Replace("'", "''")}','{tkx.KASIR06.Replace("'", "''")}','{tkx.PRAMU01.Replace("'", "''")}','{tkx.PRAMU02.Replace("'", "''")}',
                                            '{tkx.PRAMU03.Replace("'", "''")}','{tkx.PRAMU04.Replace("'", "''")}','{tkx.PRAMU05.Replace("'", "''")}','{tkx.PRAMU06.Replace("'", "''")}',
                                            '{tkx.PRAMU07.Replace("'", "''")}','{tkx.PRAMU08.Replace("'", "''")}','{tkx.PRAMU09.Replace("'", "''")}','{tkx.PRAMU10.Replace("'", "''")}',
                                            '{tkx.PRAMU11.Replace("'", "''")}','{tkx.PRAMU12.Replace("'", "''")}','{tkx.FR_MAX_CR.Replace("'", "''")}','{tkx.FR_BEGBAL.Replace("'", "''")}',
                                            '{tkx.FR_CREDIT.Replace("'", "''")}','{tkx.FR_PPN.Replace("'", "''")}','{tkx.FR_PAY.Replace("'", "''")}','{tkx.FR_PPN1.Replace("'", "''")}',
                                            '{tkx.FR_D_CR.Replace("'", "''")}','{tkx.FR_D_PAY.Replace("'", "''")}','{tkx.UPDATE.Replace("'", "''")}','{tkx.TOKO.Replace("'", "''")}',
                                            '{tkx.NPB.Replace("'", "''")}','{tkx.BPB.Replace("'", "''")}','{tkx.BADAN.Replace("'", "''")}','{tkx.ROYAL_1.Replace("'", "''")}',
                                            '{tkx.ROYAL_1P.Replace("'", "''")}','{tkx.ROYAL_2.Replace("'", "''")}','{tkx.ROYAL_2P.Replace("'", "''")}','{tkx.ROYAL_3.Replace("'", "''")}',
                                            '{tkx.ROYAL_3P.Replace("'", "''")}','{tkx.ROYAL_4.Replace("'", "''")}','{tkx.ROYAL_4P.Replace("'", "''")}','{tkx.BUAH.Replace("'", "''")}',
                                            '{tkx.TGL_PKM.Replace("'", "''")}','{tkx.JML_KM.Replace("'", "''")}','{tkx.ELPIJI.Replace("'", "''")}','{tkx.KDLM.Replace("'", "''")}',
                                            '{tkx.TRF.Replace("'", "''")}','{tkx.UPD_DHO.Replace("'", "''")}','{tkx.UPD_DEC.Replace("'", "''")}','{tkx.UPD_AAC.Replace("'", "''")}',
                                            '{tkx.FDISP.Replace("'", "''")}','{tkx.COMPETE.Replace("'", "''")}','{tkx.TRF_HO.Replace("'", "''")}','{tkx.OTO.Replace("'", "''")}',
                                            '{tkx.KDBR.Replace("'", "''")}','{tkx.BERLAKU.Replace("'", "''")}','{tkx.NPWP_DC.Replace("'", "''")}','{tkx.TGL_DC.Replace("'", "''")}',
                                            '{tkx.ALMT_PJK1.Replace("'", "''")}','{tkx.ALMT_PJK2.Replace("'", "''")}','{tkx.ALMT_PJK3.Replace("'", "''")}','{tkx.KODE_CID.Replace("'", "''")}',
                                            '{tkx.KD_FRC.Replace("'", "''")}','{tkx.NPPKP.Replace("'", "''")}','{tkx.MRCHNT.Replace("'", "''")}','{tkx.TOK24.Replace("'", "''")}',
                                            '{tkx.CONV.Replace("'", "''")}','{tkx.BERLAKU_HRG.Replace("'", "''")}','{tkx.BERLAKU_CID.Replace("'", "''")}','{tkx.JADWAL_CONV.Replace("'", "''")}',
                                            '{tkx.FLAGTOKO.Replace("'", "''")}','{tkx.HARI24.Replace("'", "''")}','{tkx.IMOBILE.Replace("'", "''")}','{tkx.APKA.Replace("'", "''")}',
                                            '{tkx.TIDAK_HITUNG_PB.Replace("'", "''")}','{tkx.JENIS_TOKO.Replace("'", "''")}','{tkx.CLASS_TK.Replace("'", "''")}','{tkx.SUBCLASS_TK.Replace("'", "''")}',
                                            '{tkx.TGL_AKTIF_MSTORE.Replace("'", "''")}','{tkx.KONEKSI_PRIMARY.Replace("'", "''")}','{tkx.KONEKSI_SECONDARY.Replace("'", "''")}',
                                            '{tkx.TANGGAL_SECONDARY.Replace("'", "''")}','{tkx.TANGGAL_PRIMARY.Replace("'", "''")}','{tkx.DOMAIN.Replace("'", "''")}',
                                            '{tkx.DUAL_SCREEN.Replace("'", "''")}','{tkx.START_TGL_BERLAKU_SE.Replace("'", "''")}','{tkx.END_TGL_BERLAKU_SE.Replace("'", "''")}',
                                            '{tkx.FLAG_LIBUR_RUTIN.Replace("'", "''")}','{tkx.JAM_BUKA.Replace("'", "''")}','{tkx.JAM_TUTUP.Replace("'", "''")}','{tkx.POINT_CAFE.Replace("'", "''")}',
                                            '{tkx.EVENT.Replace("'", "''")}','{tkx.PERIODE_AWAL.Replace("'", "''")}','{tkx.PERIODE_AKHIR.Replace("'", "''")}',
                                            '{tkx.TGL_BUKA_POINTCAFE.Replace("'", "''")}','{tkx.TGL_TUTUP_POINTCAFE.Replace("'", "''")}','{tkx.TGL_DIS05.Replace("'", "''")}',
                                            '{tkx.KABUPATEN.Replace("'", "''")}','{tkx.SETOR_BANK.Replace("'", "''")}','{tkx.NAMA_BANK1.Replace("'", "''")}',
                                            '{tkx.TGL_AWAL_BANK1.Replace("'", "''")}','{tkx.TGL_AKHIR_BANK1.Replace("'", "''")}','{tkx.NAMA_BANK2.Replace("'", "''")}',
                                            '{tkx.TGL_AWAL_BANK2.Replace("'", "''")}','{tkx.TGL_AKHIR_BANK2.Replace("'", "''")}','{tkx.TOK_PERDA.Replace("'", "''")}',
                                            '{tkx.TOK_TGL_AWAL_PERDA.Replace("'", "''")}','{tkx.TOK_TGL_AKHIR_PERDA.Replace("'", "''")}','{tkx.TOK_POINTCAFFEE_APP.Replace("'", "''")}',
                                            '{tkx.TOK_DRIVE_THRU.Replace("'", "''")}','{tkx.ALMT_TOKO.Replace("'", "''")}','{tkx.ALMT_PAJAK.Replace("'", "''")}','{tkx.TOK_PT_BIP.Replace("'", "''")}',
                                            '{tkx.FLAGTK.Replace("'", "''")}','{tkx.MARKUP_IGR.Replace("'", "''")}','{tkx.TGL_MULTIRATES.Replace("'", "''")}','{tkx.NITKU.Replace("'", "''")}',
                                            '{tglStatus:yyyy-MM-dd HH:mm:ss}','{tglProses:yyyy-MM-dd HH:mm:ss}',USER(),NOW()),");

                        iCount++;

                        if (iCount == countIns)
                        {
                            // Hapus trailing koma lalu eksekusi
                            sqlBuild.Length--;
                            sql = sqlBuild.ToString();
                            await dtService.ExecuteAsync(sql);

                            // Reset untuk batch berikutnya
                            iCount = 0;
                            sqlBuild = new StringBuilder(insertHeader);
                        }
                    }

                    // Eksekusi sisa data yang belum mencapai batchSize
                    if (iCount > 0)
                    {
                        sqlBuild.Length--;
                        sql = sqlBuild.ToString();
                        await dtService.ExecuteAsync(sql);
                    }

                }
                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return false;
            }
        }
        public async Task<bool> InsertMasterDCX(string jobs, List<DCX> objDCX, DateTime tglStatus, DateTime tglProses)
        {
            try
            {
                using var dtService = db.CreateConnection();
                var dbName = db.DatabaseName();
                int countIns = 50;
                int iCount = 0;
                string sql = string.Empty;


                sql = $"CREATE TABLE IF NOT EXISTS `{dbName}`.`master_all_dcx` (                                                                   " +
                      $"`KODE_TOKO` VARCHAR(25) DEFAULT '',`NAMA_TOKO` VARCHAR(200) DEFAULT '',                                                    " +
                      $"`KODE_DC` VARCHAR(25) DEFAULT '',`NAMA_DC` VARCHAR(100) DEFAULT '',                                                        " +
                      $"`KODE_CABANG` VARCHAR(25) DEFAULT '',`NAMA_CABANG` VARCHAR(50) NOT NULL,                                                   " +
                      $"`KODE_CABANG_OLD` VARCHAR(25) DEFAULT '',`NAMA_CABANG_OLD` VARCHAR(50) DEFAULT '',                                         " +
                      $"`TYPE_DC` VARCHAR(25) DEFAULT '',`TGL_AWAL` VARCHAR(25) DEFAULT '',                                                        " +
                      $"`TGL_AKHIR` VARCHAR(25) DEFAULT '',`MARK_UP_IGR` VARCHAR(25) DEFAULT '',                                                   " +
                      $"`FLAGPROD` VARCHAR(4000) DEFAULT '',                                                                                       " +
                      $"`TGL_STATUS` DATETIME DEFAULT NULL,`TGL_PROSES` DATETIME DEFAULT NULL,                                                     " +
                      $"`UPDID_CABANG` VARCHAR(100) DEFAULT NULL,`UPDTIME_CABANG` DATETIME DEFAULT NULL,                                           " +
                      $"PRIMARY KEY (`KODE_TOKO`,`KODE_DC`,`NAMA_DC`,`KODE_CABANG`,`NAMA_CABANG`),                                                 " +
                      $"KEY `IDX_TGL_STATUS` (`TGL_STATUS`),                                                                                       " +
                      $"KEY `IDX_TGL_PROSES` (`TGL_PROSES`)                                                                                        " +
                      $") ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await dtService.ExecuteAsync(sql);

                sql = $"TRUNCATE `{dbName}`.`master_all_dcx`;";
                await dtService.ExecuteAsync(sql);

                if (objDCX.Count > 0)
                {
                    const string columns = $"(`KODE_TOKO`, `NAMA_TOKO`, `KODE_DC`, `NAMA_DC`, `KODE_CABANG`, `NAMA_CABANG`, `KODE_CABANG_OLD`, `NAMA_CABANG_OLD`, `TYPE_DC`, " +
                                           $"`TGL_AWAL`, `TGL_AKHIR`, `MARK_UP_IGR`, `FLAGPROD`, `TGL_STATUS`, `TGL_PROSES`, `UPDID_CABANG`, `UPDTIME_CABANG`)";

                    string insertHeader = $"INSERT IGNORE INTO `{db.DatabaseName()}`.`master_all_dcx` {columns} VALUES ";
                    var sqlBuild = new StringBuilder(insertHeader);

                    foreach (var dcx in objDCX)
                    {
                        sqlBuild.Append($@"(
                                       '{dcx.KODE_TOKO.Replace("'", "''")}', '{dcx.NAMA_TOKO.Replace("'", "''")}',
                                       '{dcx.KODE_DC.Replace("'", "''")}', '{dcx.NAMA_DC.Replace("'", "''")}',
                                       '{dcx.KODE_CABANG.Replace("'", "''")}', '{dcx.NAMA_CABANG.Replace("'", "''")}',
                                       '{dcx.KODE_CABANG_OLD.Replace("'", "''")}', '{dcx.NAMA_CABANG_OLD.Replace("'", "''")}',
                                       '{dcx.TYPE_DC.Replace("'", "''")}', '{dcx.TGL_AWAL.Replace("'", "''")}',
                                       '{dcx.TGL_AKHIR.Replace("'", "''")}', '{dcx.MARK_UP_IGR.Replace("'", "''")}',
                                       '{dcx.FLAGPROD.Replace("'", "''")}',
                                       '{tglStatus:yyyy-MM-dd HH:mm:ss}', '{tglProses:yyyy-MM-dd HH:mm:ss}', USER(), NOW()),");

                        iCount++;

                        if (iCount == countIns)
                        {
                            sqlBuild.Length--;
                            sql = sqlBuild.ToString();
                            await dtService.ExecuteAsync(sql);

                            iCount = 0;
                            sqlBuild = new StringBuilder(insertHeader);
                        }
                    }

                    if (iCount > 0)
                    {
                        sqlBuild.Length--;
                        sql = sqlBuild.ToString();
                        await dtService.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + Environment.NewLine + ex.StackTrace, Utility.TipeLog.Error);
                return false;
            }
        }
        public async Task<bool> InsertMasterDIVISI(string jobs, List<DIVISI> objDIVISI, DateTime tglStatus, DateTime tglProses)
        {
            using var dtService = db.CreateConnection();
            int countIns = 50;
            int iCount = 0;
            string sql = "";

            try
            {
                sql = $"CREATE TABLE IF NOT EXISTS `{db.DatabaseName()}`.`master_all_divisi` " +
                      $"(`DIV_KD` VARCHAR(25) DEFAULT '',`DIV_NA` VARCHAR(100) DEFAULT ''," +
                      $"`DIV_MGR` VARCHAR(100) DEFAULT ''," +
                      $"`TGL_STATUS` DATETIME DEFAULT NULL,`TGL_PROSES` DATETIME DEFAULT NULL," +
                      $"`UPDID_CABANG` VARCHAR(100) DEFAULT NULL,`UPDTIME_CABANG` DATETIME DEFAULT NULL," +
                      $"PRIMARY KEY (`DIV_KD`),KEY `IDX_TGL_STATUS` (`TGL_STATUS`),KEY `IDX_TGL_PROSES` (`TGL_PROSES`)) ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await dtService.ExecuteScalarAsync(sql);

                sql = $"TRUNCATE TABLE `{db.DatabaseName()}`.`master_all_divisi`;";
                await dtService.ExecuteScalarAsync(sql);

                if (objDIVISI.Count > 0)
                {
                    const string columns = "(`DIV_KD`, `DIV_NA`, `DIV_MGR`, `TGL_STATUS`, `TGL_PROSES`, `UPDID_CABANG`, `UPDTIME_CABANG`)";
                    string insertHeader = $"INSERT IGNORE INTO `{db.DatabaseName()}`.`master_all_divisi` {columns} VALUES ";
                    var sqlBuild = new StringBuilder(insertHeader);

                    foreach (var div in objDIVISI)
                    {
                        sqlBuild.Append($"('{div.DIV_KD.Replace("'", "''")}', '{div.DIV_NA.Replace("'", "''")}', '{div.DIV_MGR.Replace("'", "''")}', " +
                                        $"'{tglStatus:yyyy-MM-dd HH:mm:ss}', '{tglProses:yyyy-MM-dd HH:mm:ss}', USER(), NOW()),");
                        iCount++;

                        if (iCount == countIns)
                        {
                            sqlBuild.Length--;
                            sql = sqlBuild.ToString();
                            await dtService.ExecuteAsync(sql);

                            iCount = 0;
                            sqlBuild = new StringBuilder(insertHeader);
                        }
                    }

                    if (iCount > 0)
                    {
                        sqlBuild.Length--;
                        sql = sqlBuild.ToString();
                        await dtService.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return false;
            }
        }
        public async Task<bool> InsertMasterDEPT(string jobs, List<DEPT> objDEPT, DateTime tglStatus, DateTime tglProses)
        {
            using var dtService = db.CreateConnection();
            int countIns = 50;
            int iCount = 0;
            string sql = "";

            try
            {
                sql = $"CREATE TABLE IF NOT EXISTS `{db.DatabaseName()}`.`master_all_dept` " +
                      $"(`DEP_KD` VARCHAR(25) DEFAULT '',`DEP_NA` VARCHAR(100) DEFAULT ''," +
                      $"`DIV_KD` VARCHAR(25) DEFAULT '',`DEP_MGR` VARCHAR(100) DEFAULT '',`DEP_SPV` VARCHAR(100) DEFAULT ''," +
                      $"`TGL_STATUS` DATETIME DEFAULT NULL,`TGL_PROSES` DATETIME DEFAULT NULL," +
                      $"`UPDID_CABANG` VARCHAR(100) DEFAULT NULL,`UPDTIME_CABANG` DATETIME DEFAULT NULL," +
                      $"PRIMARY KEY (`DEP_KD`, `DIV_KD`),KEY `IDX_TGL_STATUS` (`TGL_STATUS`),KEY `IDX_TGL_PROSES` (`TGL_PROSES`)) ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await dtService.ExecuteScalarAsync(sql);

                sql = $"TRUNCATE TABLE `{db.DatabaseName()}`.`master_all_dept`;";
                await dtService.ExecuteScalarAsync(sql);

                if (objDEPT.Count > 0)
                {
                    const string columns = "(`DEP_KD`, `DEP_NA`, `DIV_KD`, `DEP_MGR`, `DEP_SPV`, `TGL_STATUS`, `TGL_PROSES`, `UPDID_CABANG`, `UPDTIME_CABANG`)";
                    string insertHeader = $"INSERT IGNORE INTO `{db.DatabaseName()}`.`master_all_dept` {columns} VALUES ";
                    var sqlBuild = new StringBuilder(insertHeader);

                    foreach (var dept in objDEPT)
                    {
                        sqlBuild.Append($"('{dept.DEP_KD.Replace("'", "''")}', '{dept.DEP_NA.Replace("'", "''")}', " +
                                        $"'{dept.DIV_KD.Replace("'", "''")}', '{dept.DEP_MGR.Replace("'", "''")}', '{dept.DEP_SPV.Replace("'", "''")}', " +
                                        $"'{tglStatus:yyyy-MM-dd HH:mm:ss}', '{tglProses:yyyy-MM-dd HH:mm:ss}', USER(), NOW()),");
                        iCount++;

                        if (iCount == countIns)
                        {
                            sqlBuild.Length--;
                            sql = sqlBuild.ToString();
                            await dtService.ExecuteAsync(sql);

                            iCount = 0;
                            sqlBuild = new StringBuilder(insertHeader);
                        }
                    }

                    if (iCount > 0)
                    {
                        sqlBuild.Length--;
                        sql = sqlBuild.ToString();
                        await dtService.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return false;
            }
        }
        public async Task<bool> InsertMasterKATEGORI(string jobs, List<KATEGORI> objKAT, DateTime tglStatus, DateTime tglProses)
        {
            using var dtService = db.CreateConnection();
            int countIns = 50;
            int iCount = 0;
            string sql = "";

            try
            {
                // Cek table master all KATEGORI ada atau tidak
                sql = $"CREATE TABLE IF NOT EXISTS `{db.DatabaseName()}`.`master_all_kategori` " +
                      $"(`KAT_KD` VARCHAR(25) DEFAULT '',`KAT_NA` VARCHAR(100) DEFAULT ''," +
                      $"`DEP_KD` VARCHAR(25) DEFAULT '',`DIV_KD` VARCHAR(25) DEFAULT ''," +
                      $"`KAT_MGR` VARCHAR(50) DEFAULT '',`KAT_SPV` VARCHAR(50) DEFAULT ''," +
                      $"`TGL_STATUS` DATETIME DEFAULT NULL,`TGL_PROSES` DATETIME DEFAULT NULL," +
                      $"`UPDID_CABANG` VARCHAR(100) DEFAULT NULL,`UPDTIME_CABANG` DATETIME DEFAULT NULL," +
                      $"PRIMARY KEY (`KAT_KD`, `DEP_KD`, `DIV_KD`)," +
                      $"KEY `IDX_TGL_STATUS` (`TGL_STATUS`),KEY `IDX_TGL_PROSES` (`TGL_PROSES`)) ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await dtService.ExecuteScalarAsync(sql);

                // Truncate table master all KATEGORI
                sql = $"TRUNCATE TABLE `{db.DatabaseName()}`.`master_all_kategori`;";
                await dtService.ExecuteScalarAsync(sql);

                if (objKAT.Count > 0)
                {
                    const string columns = "(`KAT_KD`, `KAT_NA`, `DEP_KD`, `DIV_KD`, `KAT_MGR`, `KAT_SPV`, `TGL_STATUS`, `TGL_PROSES`, `UPDID_CABANG`, `UPDTIME_CABANG`)";
                    string insertHeader = $"INSERT IGNORE INTO `{db.DatabaseName()}`.`master_all_kategori` {columns} VALUES ";
                    var sqlBuild = new StringBuilder(insertHeader);

                    foreach (var kat in objKAT)
                    {
                        sqlBuild.Append($"('{kat.KAT_KD.Replace("'", "''")}', '{kat.KAT_NA.Replace("'", "''")}', '{kat.DEP_KD.Replace("'", "''")}', " +
                                        $"'{kat.DIV_KD.Replace("'", "''")}', '{kat.KAT_MGR.Replace("'", "''")}', '{kat.KAT_SPV.Replace("'", "''")}', " +
                                        $"'{tglStatus:yyyy-MM-dd HH:mm:ss}', '{tglProses:yyyy-MM-dd HH:mm:ss}', USER(), NOW()),");
                        iCount++;

                        if (iCount == countIns)
                        {
                            sqlBuild.Length--;
                            sql = sqlBuild.ToString();
                            await dtService.ExecuteAsync(sql);

                            iCount = 0;
                            sqlBuild = new StringBuilder(insertHeader);
                        }
                    }

                    if (iCount > 0)
                    {
                        sqlBuild.Length--;
                        sql = sqlBuild.ToString();
                        await dtService.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return false;
            }
        }
        public async Task<bool> InsertMasterTATPrd(string jobs, List<TAT_PRD> objTATPrd, DateTime tglStatus, DateTime tglProses)
        {
            using var dtService = db.CreateConnection();
            int countIns = 50;
            int iCount = 0;
            string sql = "";

            try
            {
                // Cek table master all TAT_PRD ada atau tidak
                sql = $"CREATE TABLE IF NOT EXISTS `{db.DatabaseName()}`.`master_all_tat_prd` " +
                      $"(`TAT_ID` VARCHAR(90) DEFAULT '',`PRDCD` VARCHAR(25) DEFAULT ''," +
                      $"`TOKO_IN` TEXT," +
                      $"`TGL_STATUS` DATETIME DEFAULT NULL,`TGL_PROSES` DATETIME DEFAULT NULL," +
                      $"`UPDID_CABANG` VARCHAR(100) DEFAULT NULL,`UPDTIME_CABANG` DATETIME DEFAULT NULL," +
                      $"PRIMARY KEY (`TAT_ID`, `PRDCD`)," +
                      $"KEY `IDX_TGL_STATUS` (`TGL_STATUS`),KEY `IDX_TGL_PROSES` (`TGL_PROSES`)) ENGINE=INNODB DEFAULT CHARSET=latin1;";
                await dtService.ExecuteScalarAsync(sql);

                // Truncate table master all TAT_PRD
                sql = $"TRUNCATE TABLE `{db.DatabaseName()}`.`master_all_tat_prd`;";
                await dtService.ExecuteScalarAsync(sql);

                if (objTATPrd.Count > 0)
                {
                    const string columns = "(`TAT_ID`, `PRDCD`, `TOKO_IN`, `TGL_STATUS`, `TGL_PROSES`, `UPDID_CABANG`, `UPDTIME_CABANG`)";
                    string insertHeader = $"INSERT IGNORE INTO `{db.DatabaseName()}`.`master_all_tat_prd` {columns} VALUES ";
                    var sqlBuild = new StringBuilder(insertHeader);

                    foreach (var tat in objTATPrd)
                    {
                        sqlBuild.Append($"('{tat.TAT_ID.Replace("'", "''")}', '{tat.PRDCD.Replace("'", "''")}', '{tat.TOKO_IN.Replace("'", "''")}', " +
                                        $"'{tglStatus:yyyy-MM-dd HH:mm:ss}', '{tglProses:yyyy-MM-dd HH:mm:ss}', USER(), NOW()),");
                        iCount++;

                        if (iCount == countIns)
                        {
                            sqlBuild.Length--;
                            sql = sqlBuild.ToString();
                            await dtService.ExecuteAsync(sql);

                            iCount = 0;
                            sqlBuild = new StringBuilder(insertHeader);
                        }
                    }

                    if (iCount > 0)
                    {
                        sqlBuild.Length--;
                        sql = sqlBuild.ToString();
                        await dtService.ExecuteAsync(sql);
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return false;
            }
        }
        public async Task<List<LogMasterTKXDCX>> GetPartisipanLogMaster_TKX_DCX(string jobs, string strListToko)
        {
            using var dtService = db.CreateConnection();
            string sql = "";

            try
            {
                sql = $"SELECT `IDX`.KODETOKO, CAST(DATE_FORMAT(`IDX`.`TGL_PROSES`,'%Y-%m-%d %H:%i:%s') AS CHAR) AS `TGL_PROSES`," +
                      $" CAST(DATE_FORMAT(`IDX`.`TGL_STATUS`,'%Y-%m-%d') AS CHAR) AS `TGL_STATUS`, `TKX`.`JENIS` AS `JENIS_TKX`, `DCX`.`JENIS` AS `JENIS_DCX`" +
                      $" FROM (" +
                      $" SELECT `KODETOKO`,`TGL_PROSES`,`TGL_STATUS`" +
                      $" FROM `{db.DatabaseName()}`.`log_master`" +
                      $" WHERE `JENIS` = 'TKX' AND `SEND` IS NULL AND `KODETOKO` IN ({strListToko})" +
                      $" UNION" +
                      $" SELECT `KODETOKO`,`TGL_PROSES`,`TGL_STATUS`" +
                      $" FROM `{db.DatabaseName()}`.`log_master`" +
                      $" WHERE `JENIS` = 'DCX' AND `SEND` IS NULL AND `KODETOKO` IN ({strListToko})" +
                      $") `IDX`" +
                      $" LEFT JOIN (SELECT * FROM `{db.DatabaseName()}`.`log_master` WHERE `JENIS` = 'TKX' AND `SEND` IS NULL) `TKX`" +
                      $" ON `IDX`.`KODETOKO` = `TKX`.`KODETOKO` AND `IDX`.`TGL_PROSES` = `TKX`.`TGL_PROSES`" +
                      $" LEFT JOIN (SELECT * FROM `{db.DatabaseName()}`.`log_master` WHERE `JENIS` = 'DCX' AND `SEND` IS NULL) `DCX`" +
                      $" ON `IDX`.`KODETOKO` = `DCX`.`KODETOKO` AND `IDX`.`TGL_PROSES` = `DCX`.`TGL_PROSES`" +
                      $" ORDER BY `TGL_PROSES` DESC;";

                var result = await dtService.QueryAsync<LogMasterTKXDCX>(sql);
                return result.ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return new List<LogMasterTKXDCX>();
            }
        }
        public async Task<List<LogMasterDIVDEPTKATE>> GetPartisipanLogMaster_DIV_DEPT_KATE(string jobs, string strListToko)
        {
            using var dtService = db.CreateConnection();
            string sql = "";

            try
            {
                sql = $"SELECT `IDX`.KODETOKO, CAST(DATE_FORMAT(`IDX`.`TGL_PROSES`,'%Y-%m-%d %H:%i:%s') AS CHAR) AS `TGL_PROSES`," +
                      $" CAST(DATE_FORMAT(`IDX`.`TGL_STATUS`,'%Y-%m-%d') AS CHAR) AS `TGL_STATUS`," +
                      $" `DIV`.`JENIS` AS `JENIS_DIV`, `DEPT`.`JENIS` AS `JENIS_DEPT`, `KATE`.`JENIS` AS `JENIS_KATE`" +
                      $" FROM (" +
                      $" SELECT `KODETOKO`,`TGL_PROSES`,`TGL_STATUS`" +
                      $" FROM `{db.DatabaseName()}`.`log_master`" +
                      $" WHERE `JENIS` = 'DIVISI' AND `SEND` IS NULL AND `KODETOKO` IN ({strListToko})" +
                      $" UNION" +
                      $" SELECT `KODETOKO`,`TGL_PROSES`,`TGL_STATUS`" +
                      $" FROM `{db.DatabaseName()}`.`log_master`" +
                      $" WHERE `JENIS` = 'DEPT' AND `SEND` IS NULL AND `KODETOKO` IN ({strListToko})" +
                      $" UNION" +
                      $" SELECT `KODETOKO`,`TGL_PROSES`,`TGL_STATUS`" +
                      $" FROM `{db.DatabaseName()}`.`log_master`" +
                      $" WHERE `JENIS` = 'KATEGORI' AND `SEND` IS NULL AND `KODETOKO` IN ({strListToko})" +
                      $") `IDX`" +
                      $" LEFT JOIN (SELECT * FROM `{db.DatabaseName()}`.`log_master` WHERE `JENIS` = 'DIVISI' AND `SEND` IS NULL) `DIV`" +
                      $" ON `IDX`.`KODETOKO` = `DIV`.`KODETOKO` AND `IDX`.`TGL_PROSES` = `DIV`.`TGL_PROSES`" +
                      $" LEFT JOIN (SELECT * FROM `{db.DatabaseName()}`.`log_master` WHERE `JENIS` = 'DEPT' AND `SEND` IS NULL) `DEPT`" +
                      $" ON `IDX`.`KODETOKO` = `DEPT`.`KODETOKO` AND `IDX`.`TGL_PROSES` = `DEPT`.`TGL_PROSES`" +
                      $" LEFT JOIN (SELECT * FROM `{db.DatabaseName()}`.`log_master` WHERE `JENIS` = 'KATEGORI' AND `SEND` IS NULL) `KATE`" +
                      $" ON `IDX`.`KODETOKO` = `KATE`.`KODETOKO` AND `IDX`.`TGL_PROSES` = `KATE`.`TGL_PROSES`" +
                      $" ORDER BY `TGL_PROSES` DESC;";

                var result = await dtService.QueryAsync<LogMasterDIVDEPTKATE>(sql);
                return result.ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return new List<LogMasterDIVDEPTKATE>();
            }
        }
        public async Task<List<LogMasterBase>> GetPartisipanLogMaster_TAT_PRD(string jobs, string strListToko)
        {
            using var dtService = db.CreateConnection();
            string sql = "";

            try
            {
                sql = $"SELECT `KODETOKO`, CAST(DATE_FORMAT(`TGL_STATUS`,'%Y-%m-%d') AS CHAR) AS `TGL_STATUS`," +
                      $" CAST(DATE_FORMAT(`TGL_PROSES`,'%Y-%m-%d %H:%i:%s') AS CHAR) AS `TGL_PROSES`" +
                      $" FROM `{db.DatabaseName()}`.`log_master`" +
                      $" WHERE `JENIS` = 'TAT_PRD' AND `SEND` IS NULL AND `KODETOKO` IN ({strListToko})" +
                      $" ORDER BY `TGL_PROSES` DESC;";

                var result = await dtService.QueryAsync<LogMasterBase>(sql);
                return result.ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return new List<LogMasterBase>();
            }
        }
        public async Task<List<TAT_PRD>> GetAllMaster_TAT_PRD(string jobs)
        {
            using var dtService = db.CreateConnection();
            string sql = "";

            try
            {
                sql = $"SELECT `TAT_ID`, `PRDCD`, `TOKO_IN`," +
                      $" CAST(DATE_FORMAT(`TGL_STATUS`,'%Y-%m-%d') AS CHAR) AS `TGL_STATUS`," +
                      $" CAST(DATE_FORMAT(`TGL_PROSES`,'%Y-%m-%d %H:%i:%s') AS CHAR) AS `TGL_PROSES`" +
                      $" FROM `{db.DatabaseName()}`.`master_all_tat_prd`;";

                var result = await dtService.QueryAsync<TAT_PRD>(sql);
                return result.ToList();
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return new List<TAT_PRD>();
            }
        }
        public async Task<string> CreateQueryTkxCab(string jobs)
        {
            using var dtService = db.CreateConnection();
            var sqlBuild = new StringBuilder();
            string sql = "";

            try
            {
                var dtTKX = (await dtService.QueryAsync<dynamic>(
                    $"SELECT DISTINCT `KDTK`, `NAMA`, `KOTA` FROM `{db.DatabaseName()}`.`master_all_tkx`;")).ToList();

                if (dtTKX.Count > 0)
                {
                    // Create Table `temp_tkx_cabang_ws`
                    sqlBuild.Append("CREATE TABLE IF NOT EXISTS `pos`.`temp_tkx_cabang_ws` " +
                                    "(`KODETOKO` VARCHAR(5) NOT NULL, `NAMATOKO` VARCHAR(50) DEFAULT '', " +
                                    "`KOTA` VARCHAR(50) DEFAULT '', `ADDID` VARCHAR(50) DEFAULT NULL, `ADDTIME` DATETIME DEFAULT NULL, " +
                                    "PRIMARY KEY (`KODETOKO`)) ENGINE=INNODB DEFAULT CHARSET=latin1; " +
                                    "TRUNCATE `pos`.`temp_tkx_cabang_ws`;");

                    // Query Insert `temp_tkx_cabang_ws`
                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_tkx_cabang_ws` (`KODETOKO`, `NAMATOKO`, `KOTA`, `ADDID`, `ADDTIME`) VALUES ");

                    foreach (var row in dtTKX)
                    {
                        string kdtk = ((string)row.KDTK).Replace("'", "''");
                        string nama = ((string)row.NAMA).Replace("'", "''");
                        string kota = ((string)row.KOTA).Replace("'", "''");
                        sqlBuild.Append($"('{kdtk}', '{nama}', '{kota}', USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sql = sqlBuild.ToString() + ";";
                    sqlBuild.Clear();
                }

                return sql;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return "";
            }
        }
        public async Task<string> CreateQueryTKX_ByTOKO(string jobs, string kodeToko, string tglProses, bool setIsSend)
        {
            using var dtService = db.CreateConnection();
            var sqlBuild = new StringBuilder();
            string sql = "";

            try
            {
                var dtTKX = (await dtService.QueryAsync<TKX>(
                    $"SELECT * FROM `{db.DatabaseName()}`.`master_all_tkx` " +
                    $"WHERE `KDTK` = '{kodeToko}' AND `TGL_PROSES` = '{tglProses}';")).ToList();

                if (dtTKX.Count > 0)
                {
                    setIsSend = true;

                    // DROP & CREATE Table `temp_tkx_ws`
                    // Revisi (17 Juli 2023) : Ubah tipe data FLAGTK dari VARCHAR[200] menjadi TEXT
                    // Revisi (18 Desember 2024) : Tambah data kolom NITKU
                    sqlBuild.Append("DROP TABLE IF EXISTS `pos`.`temp_tkx_ws`; " +
                                    "CREATE TABLE IF NOT EXISTS `pos`.`temp_tkx_ws` (" +
                                    "`RECID1` VARCHAR(25) DEFAULT '',`RECID` VARCHAR(25) DEFAULT '',`KDSTRATA` VARCHAR(25) DEFAULT '',`KDORGAN` VARCHAR(25) DEFAULT '',`KDLOC` VARCHAR(50) DEFAULT '',`KDTK` VARCHAR(25) NOT NULL," +
                                    "`NAMA` VARCHAR(200) DEFAULT '',`ALMT` VARCHAR(500) DEFAULT '',`KOTA` VARCHAR(100) DEFAULT '',`RT` VARCHAR(25) DEFAULT '',`RW` VARCHAR(25) DEFAULT''," +
                                    "`TELP` VARCHAR(25) DEFAULT '',`TELP_1` VARCHAR(25) DEFAULT '',`TELP_2` VARCHAR(25) DEFAULT '',`TELP_3` VARCHAR(25) DEFAULT '',`FAX_1` VARCHAR(25) DEFAULT '',`FAX_2` VARCHAR(25) DEFAULT '',`FAX_3` VARCHAR(25) DEFAULT ''," +
                                    "`KODE_POS` VARCHAR(25) DEFAULT '',`GATE` VARCHAR(25) DEFAULT '',`COMP` VARCHAR(25) DEFAULT '',`LUAS` VARCHAR(25) DEFAULT '',`BUKA` VARCHAR(25) DEFAULT '',`MODE` VARCHAR(25) DEFAULT '',`TYPE_TOKO` VARCHAR(25) DEFAULT '',`FOT` VARCHAR(25) DEFAULT ''," +
                                    "`TYPE_HARGA` VARCHAR(25) DEFAULT '',`TYPE_RAK` VARCHAR(25) DEFAULT '',`LS_TANAH` VARCHAR(25) DEFAULT '',`LS_JUAL` VARCHAR(25) DEFAULT '',`LS_DISP` VARCHAR(25) DEFAULT '',`LS_GUDANG` VARCHAR(25) DEFAULT''," +
                                    "`PKP` VARCHAR(25) DEFAULT '',`NPWP` VARCHAR(50) DEFAULT '',`SKP` VARCHAR(50) DEFAULT '',`TGLSKP` VARCHAR(25) DEFAULT '',`TUTUP` VARCHAR(25) DEFAULT '',`KIRIM` VARCHAR(25) DEFAULT '',`GUDANG` VARCHAR(25) DEFAULT ''," +
                                    "`KONT_AWAL` VARCHAR(25) DEFAULT '',`KONT_AKHIR` VARCHAR(25) DEFAULT '',`CNT_HAL` VARCHAR(25) DEFAULT '',`CNT_LT` VARCHAR(25) DEFAULT '',`COC` VARCHAR(25) DEFAULT '',`COC_TIANG` VARCHAR(25) DEFAULT ''," +
                                    "`END_GOND` VARCHAR(25) DEFAULT '',`JMLRAK` VARCHAR(25) DEFAULT '',`FLDISP` VARCHAR(25) DEFAULT '',`FREEGDL` VARCHAR(25) DEFAULT '',`PRPLPRM` VARCHAR(25) DEFAULT '',`PRMAREA` VARCHAR(25) DEFAULT ''," +
                                    "`SEWA` VARCHAR(25) DEFAULT '',`KURS` VARCHAR(25) DEFAULT '',`UMUR` VARCHAR(25) DEFAULT '',`NAMAFRC` VARCHAR(100) DEFAULT '',`MARKUP` VARCHAR(25) DEFAULT '',`KPP` VARCHAR(25) DEFAULT ''," +
                                    "`PERSONIL` VARCHAR(25) DEFAULT '',`KA_TOKO` VARCHAR(25) DEFAULT '',`AS_TOKO` VARCHAR(25) DEFAULT ''," +
                                    "`MERCHAN01` VARCHAR(25) DEFAULT '',`MERCHAN02` VARCHAR(25) DEFAULT '',`MERCHAN03` VARCHAR(25) DEFAULT '',`MERCHAN04` VARCHAR(25) DEFAULT''," +
                                    "`KASIR01` VARCHAR(25) DEFAULT '',`KASIR02` VARCHAR(25) DEFAULT '',`KASIR03` VARCHAR(25) DEFAULT '',`KASIR04` VARCHAR(25) DEFAULT '',`KASIR05` VARCHAR(25) DEFAULT '',`KASIR06` VARCHAR(25) DEFAULT ''," +
                                    "`PRAMU01` VARCHAR(25) DEFAULT '',`PRAMU02` VARCHAR(25) DEFAULT '',`PRAMU03` VARCHAR(25) DEFAULT '',`PRAMU04` VARCHAR(25) DEFAULT '',`PRAMU05` VARCHAR(25) DEFAULT '',`PRAMU06` VARCHAR(25) DEFAULT ''," +
                                    "`PRAMU07` VARCHAR(25) DEFAULT '',`PRAMU08` VARCHAR(25) DEFAULT '',`PRAMU09` VARCHAR(25) DEFAULT '',`PRAMU10` VARCHAR(25) DEFAULT '',`PRAMU11` VARCHAR(25) DEFAULT '',`PRAMU12` VARCHAR(25) DEFAULT ''," +
                                    "`FR_MAX_CR` VARCHAR(25) DEFAULT '',`FR_BEGBAL` VARCHAR(25) DEFAULT '',`FR_CREDIT` VARCHAR(25) DEFAULT '',`FR_PPN` VARCHAR(25) DEFAULT '',`FR_PAY` VARCHAR(25) DEFAULT ''," +
                                    "`FR_PPN1` VARCHAR(25) DEFAULT '',`FR_D_CR` VARCHAR(25) DEFAULT '',`FR_D_PAY` VARCHAR(25) DEFAULT '',`UPDATE` VARCHAR(25) DEFAULT '',`TOKO` VARCHAR(25) DEFAULT ''," +
                                    "`NPB` VARCHAR(25) DEFAULT '',`BPB` VARCHAR(25) DEFAULT '',`BADAN` VARCHAR(25) DEFAULT '',`ROYAL_1` VARCHAR(25) DEFAULT '',`ROYAL_1P` VARCHAR(25) DEFAULT ''," +
                                    "`ROYAL_2` VARCHAR(25) DEFAULT '',`ROYAL_2P` VARCHAR(25) DEFAULT '',`ROYAL_3` VARCHAR(25) DEFAULT '',`ROYAL_3P` VARCHAR(25) DEFAULT '',`ROYAL_4` VARCHAR(25) DEFAULT '',`ROYAL_4P` VARCHAR(25) DEFAULT ''," +
                                    "`BUAH` VARCHAR(25) DEFAULT '',`TGL_PKM` VARCHAR(25) DEFAULT '',`JML_KM` VARCHAR(25) DEFAULT '',`ELPIJI` VARCHAR(25) DEFAULT '',`KDLM` VARCHAR(25) DEFAULT '',`TRF` VARCHAR(25) DEFAULT ''," +
                                    "`UPD_DHO` VARCHAR(25) DEFAULT '',`UPD_DEC` VARCHAR(25) DEFAULT '',`UPD_AAC` VARCHAR(25) DEFAULT '',`FDISP` VARCHAR(25) DEFAULT '',`COMPETE` VARCHAR(50) DEFAULT ''," +
                                    "`TRF_HO` VARCHAR(25) DEFAULT '',`OTO` VARCHAR(25) DEFAULT '',`KDBR` VARCHAR(25) DEFAULT '',`BERLAKU` VARCHAR(25) DEFAULT '',`NPWP_DC` VARCHAR(25) DEFAULT '',`TGL_DC` VARCHAR(25) DEFAULT ''," +
                                    "`ALMT_PJK1` VARCHAR(500) DEFAULT '',`ALMT_PJK2` VARCHAR(500) DEFAULT '',`ALMT_PJK3` VARCHAR(500) DEFAULT ''," +
                                    "`KODE_CID` VARCHAR(25) DEFAULT '',`KD_FRC` VARCHAR(25) DEFAULT '',`NPPKP` VARCHAR(25) DEFAULT '',`MRCHNT` VARCHAR(25) DEFAULT '',`TOK24` VARCHAR(25) DEFAULT ''," +
                                    "`CONV` VARCHAR(25) DEFAULT '',`BERLAKU_HRG` VARCHAR(25) DEFAULT '',`BERLAKU_CID` VARCHAR(25) DEFAULT '',`JADWAL_CONV` VARCHAR(25) DEFAULT ''," +
                                    "`FLAGTOKO` VARCHAR(50) DEFAULT '',`HARI24` VARCHAR(25) DEFAULT '',`IMOBILE` VARCHAR(25) DEFAULT '',`APKA` VARCHAR(25) DEFAULT '',`TIDAK_HITUNG_PB` VARCHAR(25) DEFAULT ''," +
                                    "`JENIS_TOKO` VARCHAR(25) DEFAULT '',`CLASS_TK` VARCHAR(25) DEFAULT '',`SUBCLASS_TK` VARCHAR(25) DEFAULT '',`TGL_AKTIF_MSTORE` VARCHAR(25) DEFAULT ''," +
                                    "`KONEKSI_PRIMARY` VARCHAR(25) DEFAULT '',`KONEKSI_SECONDARY` VARCHAR(25) DEFAULT '',`TANGGAL_SECONDARY` VARCHAR(25) DEFAULT '',`TANGGAL_PRIMARY` VARCHAR(25) DEFAULT ''," +
                                    "`DOMAIN` VARCHAR(25) DEFAULT '',`DUAL_SCREEN` VARCHAR(25) DEFAULT '',`START_TGL_BERLAKU_SE` VARCHAR(25) DEFAULT '',`END_TGL_BERLAKU_SE` VARCHAR(25) DEFAULT ''," +
                                    "`FLAG_LIBUR_RUTIN` VARCHAR(25) DEFAULT '',`JAM_BUKA` VARCHAR(25) DEFAULT '',`JAM_TUTUP` VARCHAR(25) DEFAULT '',`POINT_CAFE` VARCHAR(5) DEFAULT ''," +
                                    "`EVENT` VARCHAR(25) DEFAULT '',`PERIODE_AWAL` VARCHAR(25) DEFAULT '',`PERIODE_AKHIR` VARCHAR(25) DEFAULT ''," +
                                    "`TGL_BUKA_POINTCAFE` VARCHAR(25) DEFAULT '',`TGL_TUTUP_POINTCAFE` VARCHAR(25) DEFAULT '',`TGL_DIS05` VARCHAR(25) DEFAULT '',`KABUPATEN` VARCHAR(50) DEFAULT ''," +
                                    "`SETOR_BANK` VARCHAR(25) DEFAULT '',`NAMA_BANK1` VARCHAR(100) DEFAULT '',`TGL_AWAL_BANK1` VARCHAR(25) DEFAULT '',`TGL_AKHIR_BANK1` VARCHAR(25) DEFAULT ''," +
                                    "`NAMA_BANK2` VARCHAR(100) DEFAULT '',`TGL_AWAL_BANK2` VARCHAR(25) DEFAULT '',`TGL_AKHIR_BANK2` VARCHAR(25) DEFAULT '',`TOK_PERDA` VARCHAR(25) DEFAULT ''," +
                                    "`TOK_TGL_AWAL_PERDA` VARCHAR(25) DEFAULT '',`TOK_TGL_AKHIR_PERDA` VARCHAR(25) DEFAULT '',`TOK_POINTCAFFEE_APP` VARCHAR(25) DEFAULT '',`TOK_DRIVE_THRU` VARCHAR(25) DEFAULT ''," +
                                    "`ALMT_TOKO` VARCHAR(500) DEFAULT '',`ALMT_PAJAK` VARCHAR(500) DEFAULT '',`TOK_PT_BIP` VARCHAR(25) DEFAULT '',`FLAGTK` TEXT," +
                                    "`MARKUP_IGR` VARCHAR(300) DEFAULT '',`TGL_MULTIRATES` VARCHAR(50) DEFAULT '',`NITKU` VARCHAR(50) DEFAULT ''," +
                                    "`ADDID` VARCHAR(100) DEFAULT NULL,`ADDTIME` DATETIME DEFAULT NULL," +
                                    "PRIMARY KEY (`KDTK`)) ENGINE=INNODB DEFAULT CHARSET=latin1; ");

                    // Query Insert `temp_tkx_ws`
                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_tkx_ws` (`RECID1`,`RECID`,`KDSTRATA`,`KDORGAN`,`KDLOC`,`KDTK`,`NAMA`,`ALMT`,`KOTA`," +
                                    "`RT`,`RW`,`TELP`,`TELP_1`,`TELP_2`,`TELP_3`,`FAX_1`,`FAX_2`,`FAX_3`,`KODE_POS`,`GATE`,`COMP`,`LUAS`,`BUKA`,`MODE`,`TYPE_TOKO`,`FOT`," +
                                    "`TYPE_HARGA`,`TYPE_RAK`,`LS_TANAH`,`LS_JUAL`,`LS_DISP`,`LS_GUDANG`,`PKP`,`NPWP`,`SKP`,`TGLSKP`,`TUTUP`,`KIRIM`,`GUDANG`,`KONT_AWAL`,`KONT_AKHIR`," +
                                    "`CNT_HAL`,`CNT_LT`,`COC`,`COC_TIANG`,`END_GOND`,`JMLRAK`,`FLDISP`,`FREEGDL`,`PRPLPRM`,`PRMAREA`,`SEWA`,`KURS`,`UMUR`,`NAMAFRC`,`MARKUP`,`KPP`," +
                                    "`PERSONIL`,`KA_TOKO`,`AS_TOKO`,`MERCHAN01`,`MERCHAN02`,`MERCHAN03`,`MERCHAN04`,`KASIR01`,`KASIR02`,`KASIR03`,`KASIR04`,`KASIR05`,`KASIR06`," +
                                    "`PRAMU01`,`PRAMU02`,`PRAMU03`,`PRAMU04`,`PRAMU05`,`PRAMU06`,`PRAMU07`,`PRAMU08`,`PRAMU09`,`PRAMU10`,`PRAMU11`,`PRAMU12`," +
                                    "`FR_MAX_CR`,`FR_BEGBAL`,`FR_CREDIT`,`FR_PPN`,`FR_PAY`,`FR_PPN1`,`FR_D_CR`,`FR_D_PAY`,`UPDATE`,`TOKO`,`NPB`,`BPB`," +
                                    "`BADAN`,`ROYAL_1`,`ROYAL_1P`,`ROYAL_2`,`ROYAL_2P`,`ROYAL_3`,`ROYAL_3P`,`ROYAL_4`,`ROYAL_4P`," +
                                    "`BUAH`,`TGL_PKM`,`JML_KM`,`ELPIJI`,`KDLM`,`TRF`,`UPD_DHO`,`UPD_DEC`,`UPD_AAC`,`FDISP`,`COMPETE`," +
                                    "`TRF_HO`,`OTO`,`KDBR`,`BERLAKU`,`NPWP_DC`,`TGL_DC`,`ALMT_PJK1`,`ALMT_PJK2`,`ALMT_PJK3`,`KODE_CID`,`KD_FRC`,`NPPKP`,`MRCHNT`,`TOK24`," +
                                    "`CONV`,`BERLAKU_HRG`,`BERLAKU_CID`,`JADWAL_CONV`,`FLAGTOKO`,`HARI24`,`IMOBILE`,`APKA`,`TIDAK_HITUNG_PB`," +
                                    "`JENIS_TOKO`,`CLASS_TK`,`SUBCLASS_TK`,`TGL_AKTIF_MSTORE`,`KONEKSI_PRIMARY`,`KONEKSI_SECONDARY`,`TANGGAL_SECONDARY`,`TANGGAL_PRIMARY`," +
                                    "`DOMAIN`,`DUAL_SCREEN`,`START_TGL_BERLAKU_SE`,`END_TGL_BERLAKU_SE`,`FLAG_LIBUR_RUTIN`,`JAM_BUKA`,`JAM_TUTUP`,`POINT_CAFE`," +
                                    "`EVENT`,`PERIODE_AWAL`,`PERIODE_AKHIR`,`TGL_BUKA_POINTCAFE`,`TGL_TUTUP_POINTCAFE`,`TGL_DIS05`,`KABUPATEN`," +
                                    "`SETOR_BANK`,`NAMA_BANK1`,`TGL_AWAL_BANK1`,`TGL_AKHIR_BANK1`,`NAMA_BANK2`,`TGL_AWAL_BANK2`,`TGL_AKHIR_BANK2`,`TOK_PERDA`," +
                                    "`TOK_TGL_AWAL_PERDA`,`TOK_TGL_AKHIR_PERDA`,`TOK_POINTCAFFEE_APP`,`TOK_DRIVE_THRU`,`ALMT_TOKO`,`ALMT_PAJAK`,`TOK_PT_BIP`,`FLAGTK`," +
                                    "`MARKUP_IGR`,`TGL_MULTIRATES`,`NITKU`,`ADDID`,`ADDTIME`) VALUES ");

                    foreach (var tkx in dtTKX)
                    {
                        sqlBuild.Append($@"(
                    '{tkx.RECID1.Replace("'", "''")}','{tkx.RECID.Replace("'", "''")}','{tkx.KDSTRATA.Replace("'", "''")}','{tkx.KDORGAN.Replace("'", "''")}','{tkx.KDLOC.Replace("'", "''")}','{tkx.KDTK.Replace("'", "''")}','{tkx.NAMA.Replace("'", "''")}','{tkx.ALMT.Replace("'", "''")}','{tkx.KOTA.Replace("'", "''")}',
                    '{tkx.RT.Replace("'", "''")}','{tkx.RW.Replace("'", "''")}','{tkx.TELP.Replace("'", "''")}','{tkx.TELP_1.Replace("'", "''")}','{tkx.TELP_2.Replace("'", "''")}','{tkx.TELP_3.Replace("'", "''")}','{tkx.FAX_1.Replace("'", "''")}','{tkx.FAX_2.Replace("'", "''")}','{tkx.FAX_3.Replace("'", "''")}','{tkx.KODE_POS.Replace("'", "''")}','{tkx.GATE.Replace("'", "''")}','{tkx.COMP.Replace("'", "''")}','{tkx.LUAS.Replace("'", "''")}','{tkx.BUKA.Replace("'", "''")}','{tkx.MODE.Replace("'", "''")}','{tkx.TYPE_TOKO.Replace("'", "''")}','{tkx.FOT.Replace("'", "''")}',
                    '{tkx.TYPE_HARGA.Replace("'", "''")}','{tkx.TYPE_RAK.Replace("'", "''")}','{tkx.LS_TANAH.Replace("'", "''")}','{tkx.LS_JUAL.Replace("'", "''")}','{tkx.LS_DISP.Replace("'", "''")}','{tkx.LS_GUDANG.Replace("'", "''")}','{tkx.PKP.Replace("'", "''")}','{tkx.NPWP.Replace("'", "''")}','{tkx.SKP.Replace("'", "''")}','{tkx.TGLSKP.Replace("'", "''")}','{tkx.TUTUP.Replace("'", "''")}','{tkx.KIRIM.Replace("'", "''")}','{tkx.GUDANG.Replace("'", "''")}','{tkx.KONT_AWAL.Replace("'", "''")}','{tkx.KONT_AKHIR.Replace("'", "''")}',
                    '{tkx.CNT_HAL.Replace("'", "''")}','{tkx.CNT_LT.Replace("'", "''")}','{tkx.COC.Replace("'", "''")}','{tkx.COC_TIANG.Replace("'", "''")}','{tkx.END_GOND.Replace("'", "''")}','{tkx.JMLRAK.Replace("'", "''")}','{tkx.FLDISP.Replace("'", "''")}','{tkx.FREEGDL.Replace("'", "''")}','{tkx.PRPLPRM.Replace("'", "''")}','{tkx.PRMAREA.Replace("'", "''")}','{tkx.SEWA.Replace("'", "''")}','{tkx.KURS.Replace("'", "''")}','{tkx.UMUR.Replace("'", "''")}','{tkx.NAMAFRC.Replace("'", "''")}','{tkx.MARKUP.Replace("'", "''")}','{tkx.KPP.Replace("'", "''")}',
                    '{tkx.PERSONIL.Replace("'", "''")}','{tkx.KA_TOKO.Replace("'", "''")}','{tkx.AS_TOKO.Replace("'", "''")}','{tkx.MERCHAN01.Replace("'", "''")}','{tkx.MERCHAN02.Replace("'", "''")}','{tkx.MERCHAN03.Replace("'", "''")}','{tkx.MERCHAN04.Replace("'", "''")}','{tkx.KASIR01.Replace("'", "''")}','{tkx.KASIR02.Replace("'", "''")}','{tkx.KASIR03.Replace("'", "''")}','{tkx.KASIR04.Replace("'", "''")}','{tkx.KASIR05.Replace("'", "''")}','{tkx.KASIR06.Replace("'", "''")}',
                    '{tkx.PRAMU01.Replace("'", "''")}','{tkx.PRAMU02.Replace("'", "''")}','{tkx.PRAMU03.Replace("'", "''")}','{tkx.PRAMU04.Replace("'", "''")}','{tkx.PRAMU05.Replace("'", "''")}','{tkx.PRAMU06.Replace("'", "''")}','{tkx.PRAMU07.Replace("'", "''")}','{tkx.PRAMU08.Replace("'", "''")}','{tkx.PRAMU09.Replace("'", "''")}','{tkx.PRAMU10.Replace("'", "''")}','{tkx.PRAMU11.Replace("'", "''")}','{tkx.PRAMU12.Replace("'", "''")}',
                    '{tkx.FR_MAX_CR.Replace("'", "''")}','{tkx.FR_BEGBAL.Replace("'", "''")}','{tkx.FR_CREDIT.Replace("'", "''")}','{tkx.FR_PPN.Replace("'", "''")}','{tkx.FR_PAY.Replace("'", "''")}','{tkx.FR_PPN1.Replace("'", "''")}','{tkx.FR_D_CR.Replace("'", "''")}','{tkx.FR_D_PAY.Replace("'", "''")}','{tkx.UPDATE.Replace("'", "''")}','{tkx.TOKO.Replace("'", "''")}','{tkx.NPB.Replace("'", "''")}','{tkx.BPB.Replace("'", "''")}',
                    '{tkx.BADAN.Replace("'", "''")}','{tkx.ROYAL_1.Replace("'", "''")}','{tkx.ROYAL_1P.Replace("'", "''")}','{tkx.ROYAL_2.Replace("'", "''")}','{tkx.ROYAL_2P.Replace("'", "''")}','{tkx.ROYAL_3.Replace("'", "''")}','{tkx.ROYAL_3P.Replace("'", "''")}','{tkx.ROYAL_4.Replace("'", "''")}','{tkx.ROYAL_4P.Replace("'", "''")}',
                    '{tkx.BUAH.Replace("'", "''")}','{tkx.TGL_PKM.Replace("'", "''")}','{tkx.JML_KM.Replace("'", "''")}','{tkx.ELPIJI.Replace("'", "''")}','{tkx.KDLM.Replace("'", "''")}','{tkx.TRF.Replace("'", "''")}','{tkx.UPD_DHO.Replace("'", "''")}','{tkx.UPD_DEC.Replace("'", "''")}','{tkx.UPD_AAC.Replace("'", "''")}','{tkx.FDISP.Replace("'", "''")}','{tkx.COMPETE.Replace("'", "''")}',
                    '{tkx.TRF_HO.Replace("'", "''")}','{tkx.OTO.Replace("'", "''")}','{tkx.KDBR.Replace("'", "''")}','{tkx.BERLAKU.Replace("'", "''")}','{tkx.NPWP_DC.Replace("'", "''")}','{tkx.TGL_DC.Replace("'", "''")}','{tkx.ALMT_PJK1.Replace("'", "''")}','{tkx.ALMT_PJK2.Replace("'", "''")}','{tkx.ALMT_PJK3.Replace("'", "''")}','{tkx.KODE_CID.Replace("'", "''")}','{tkx.KD_FRC.Replace("'", "''")}','{tkx.NPPKP.Replace("'", "''")}','{tkx.MRCHNT.Replace("'", "''")}','{tkx.TOK24.Replace("'", "''")}',
                    '{tkx.CONV.Replace("'", "''")}','{tkx.BERLAKU_HRG.Replace("'", "''")}','{tkx.BERLAKU_CID.Replace("'", "''")}','{tkx.JADWAL_CONV.Replace("'", "''")}','{tkx.FLAGTOKO.Replace("'", "''")}','{tkx.HARI24.Replace("'", "''")}','{tkx.IMOBILE.Replace("'", "''")}','{tkx.APKA.Replace("'", "''")}','{tkx.TIDAK_HITUNG_PB.Replace("'", "''")}',
                    '{tkx.JENIS_TOKO.Replace("'", "''")}','{tkx.CLASS_TK.Replace("'", "''")}','{tkx.SUBCLASS_TK.Replace("'", "''")}','{tkx.TGL_AKTIF_MSTORE.Replace("'", "''")}','{tkx.KONEKSI_PRIMARY.Replace("'", "''")}','{tkx.KONEKSI_SECONDARY.Replace("'", "''")}','{tkx.TANGGAL_SECONDARY.Replace("'", "''")}','{tkx.TANGGAL_PRIMARY.Replace("'", "''")}',
                    '{tkx.DOMAIN.Replace("'", "''")}','{tkx.DUAL_SCREEN.Replace("'", "''")}','{tkx.START_TGL_BERLAKU_SE.Replace("'", "''")}','{tkx.END_TGL_BERLAKU_SE.Replace("'", "''")}','{tkx.FLAG_LIBUR_RUTIN.Replace("'", "''")}','{tkx.JAM_BUKA.Replace("'", "''")}','{tkx.JAM_TUTUP.Replace("'", "''")}','{tkx.POINT_CAFE.Replace("'", "''")}',
                    '{tkx.EVENT.Replace("'", "''")}','{tkx.PERIODE_AWAL.Replace("'", "''")}','{tkx.PERIODE_AKHIR.Replace("'", "''")}','{tkx.TGL_BUKA_POINTCAFE.Replace("'", "''")}','{tkx.TGL_TUTUP_POINTCAFE.Replace("'", "''")}','{tkx.TGL_DIS05.Replace("'", "''")}','{tkx.KABUPATEN.Replace("'", "''")}',
                    '{tkx.SETOR_BANK.Replace("'", "''")}','{tkx.NAMA_BANK1.Replace("'", "''")}','{tkx.TGL_AWAL_BANK1.Replace("'", "''")}','{tkx.TGL_AKHIR_BANK1.Replace("'", "''")}','{tkx.NAMA_BANK2.Replace("'", "''")}','{tkx.TGL_AWAL_BANK2.Replace("'", "''")}','{tkx.TGL_AKHIR_BANK2.Replace("'", "''")}','{tkx.TOK_PERDA.Replace("'", "''")}',
                    '{tkx.TOK_TGL_AWAL_PERDA.Replace("'", "''")}','{tkx.TOK_TGL_AKHIR_PERDA.Replace("'", "''")}','{tkx.TOK_POINTCAFFEE_APP.Replace("'", "''")}','{tkx.TOK_DRIVE_THRU.Replace("'", "''")}','{tkx.ALMT_TOKO.Replace("'", "''")}','{tkx.ALMT_PAJAK.Replace("'", "''")}','{tkx.TOK_PT_BIP.Replace("'", "''")}','{tkx.FLAGTK.Replace("'", "''")}',
                    '{tkx.MARKUP_IGR.Replace("'", "''")}','{tkx.TGL_MULTIRATES.Replace("'", "''")}','{tkx.NITKU.Replace("'", "''")}',
                    USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sql = sqlBuild.ToString() + ";";
                    sqlBuild.Clear();
                }
                else
                {
                    setIsSend = false;
                }

                return sql;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return "";
            }
        }
        public async Task<string> CreateQueryDCX_ByTOKO(string jobs, string kodeToko, string tglProses, bool setIsSend)
        {
            using var dtService = db.CreateConnection();
            var sqlBuild = new StringBuilder();
            string sql = "";

            try
            {
                var dtDCX = (await dtService.QueryAsync<DCX>(
                    $"SELECT * FROM `{db.DatabaseName()}`.`master_all_dcx` " +
                    $"WHERE `KODE_TOKO` = '{kodeToko}' AND `TGL_PROSES` = '{tglProses}';")).ToList();

                if (dtDCX.Count > 0)
                {
                    setIsSend = true;

                    // DROP & CREATE Table `temp_dcx_ws`
                    sqlBuild.Append("DROP TABLE IF EXISTS `pos`.`temp_dcx_ws`; " +
                                    "CREATE TABLE IF NOT EXISTS `pos`.`temp_dcx_ws` (" +
                                    "`KODE_TOKO` VARCHAR(25) DEFAULT '',`NAMA_TOKO` VARCHAR(200) DEFAULT ''," +
                                    "`KODE_DC` VARCHAR(25) DEFAULT '',`NAMA_DC` VARCHAR(100) DEFAULT ''," +
                                    "`KODE_CABANG` VARCHAR(25) DEFAULT '',`NAMA_CABANG` VARCHAR(50) NOT NULL," +
                                    "`KODE_CABANG_OLD` VARCHAR(25) DEFAULT '',`NAMA_CABANG_OLD` VARCHAR(50) DEFAULT ''," +
                                    "`TYPE_DC` VARCHAR(25) DEFAULT '',`TGL_AWAL` VARCHAR(25) DEFAULT '',`TGL_AKHIR` VARCHAR(25) DEFAULT ''," +
                                    "`MARK_UP_IGR` VARCHAR(25) DEFAULT '',`FLAGPROD` VARCHAR(4000) DEFAULT ''," +
                                    "`ADDID` VARCHAR(100) DEFAULT NULL,`ADDTIME` DATETIME DEFAULT NULL," +
                                    "PRIMARY KEY (`KODE_TOKO`,`KODE_DC`,`NAMA_DC`,`KODE_CABANG`,`NAMA_CABANG`)) ENGINE=INNODB DEFAULT CHARSET=latin1; ");

                    // Query Insert `temp_dcx_ws`
                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_dcx_ws` " +
                                    "(`KODE_TOKO`,`NAMA_TOKO`,`KODE_DC`,`NAMA_DC`,`KODE_CABANG`,`NAMA_CABANG`," +
                                    "`KODE_CABANG_OLD`,`NAMA_CABANG_OLD`,`TYPE_DC`,`TGL_AWAL`,`TGL_AKHIR`,`MARK_UP_IGR`,`FLAGPROD`,`ADDID`,`ADDTIME`) VALUES ");

                    foreach (var dcx in dtDCX)
                    {
                        sqlBuild.Append($"('{dcx.KODE_TOKO.Replace("'", "''")}','{dcx.NAMA_TOKO.Replace("'", "''")}','{dcx.KODE_DC.Replace("'", "''")}','{dcx.NAMA_DC.Replace("'", "''")}', " +
                                        $"'{dcx.KODE_CABANG.Replace("'", "''")}','{dcx.NAMA_CABANG.Replace("'", "''")}','{dcx.KODE_CABANG_OLD.Replace("'", "''")}','{dcx.NAMA_CABANG_OLD.Replace("'", "''")}', " +
                                        $"'{dcx.TYPE_DC.Replace("'", "''")}','{dcx.TGL_AWAL.Replace("'", "''")}','{dcx.TGL_AKHIR.Replace("'", "''")}','{dcx.MARK_UP_IGR.Replace("'", "''")}', " +
                                        $"'{dcx.FLAGPROD.Replace("'", "''")}', USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sql = sqlBuild.ToString() + ";";
                    sqlBuild.Clear();
                }
                else
                {
                    setIsSend = false;
                }

                return sql;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return "";
            }
        }
        public async Task<string> CreateQueryDIVISI_ByTGLPROSES(string jobs, string tglProses, bool setIsSend)
        {
            using var dtService = db.CreateConnection();
            var sqlBuild = new StringBuilder();
            string sql = "";

            try
            {
                var dtDIVISI = (await dtService.QueryAsync<DIVISI>(
                    $"SELECT * FROM `{db.DatabaseName()}`.`master_all_divisi` " +
                    $"WHERE `TGL_PROSES` = '{tglProses}';")).ToList();

                if (dtDIVISI.Count > 0)
                {
                    setIsSend = true;

                    // DROP & CREATE Table `temp_divisi_ws`
                    sqlBuild.Append("DROP TABLE IF EXISTS `pos`.`temp_divisi_ws`; " +
                                    "CREATE TABLE IF NOT EXISTS `pos`.`temp_divisi_ws` (" +
                                    "`DIV_KD` VARCHAR(25) DEFAULT '',`DIV_NA` VARCHAR(100) DEFAULT ''," +
                                    "`DIV_MGR` VARCHAR(100) DEFAULT ''," +
                                    "`ADDID` VARCHAR(100) DEFAULT NULL,`ADDTIME` DATETIME DEFAULT NULL," +
                                    "PRIMARY KEY (`DIV_KD`)) ENGINE=INNODB DEFAULT CHARSET=latin1;");

                    // Query Insert `temp_divisi_ws`
                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_divisi_ws` (`DIV_KD`, `DIV_NA`, `DIV_MGR`, `ADDID`, `ADDTIME`) VALUES ");

                    foreach (var div in dtDIVISI)
                    {
                        sqlBuild.Append($"('{div.DIV_KD.Replace("'", "''")}', '{div.DIV_NA.Replace("'", "''")}', '{div.DIV_MGR.Replace("'", "''")}', USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sqlBuild.Append(";");
                    sql = sqlBuild.ToString();
                    sqlBuild.Clear();
                }
                else
                {
                    setIsSend = false;
                }

                return sql;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return "";
            }
        }
        public async Task<string> CreateQueryDEPT_ByTGLPROSES(string jobs, string tglProses, bool setIsSend)
        {
            using var dtService = db.CreateConnection();
            var sqlBuild = new StringBuilder();
            string sql = "";

            try
            {
                var dtDEPT = (await dtService.QueryAsync<DEPT>(
                    $"SELECT * FROM `{db.DatabaseName()}`.`master_all_dept` " +
                    $"WHERE `TGL_PROSES` = '{tglProses}';")).ToList();

                if (dtDEPT.Count > 0)
                {
                    setIsSend = true;

                    // DROP & CREATE Table `temp_dept_ws`
                    sqlBuild.Append("DROP TABLE IF EXISTS `pos`.`temp_dept_ws`; " +
                                    "CREATE TABLE IF NOT EXISTS `pos`.`temp_dept_ws` (" +
                                    "`DEP_KD` VARCHAR(25) DEFAULT '',`DEP_NA` VARCHAR(100) DEFAULT ''," +
                                    "`DIV_KD` VARCHAR(25) DEFAULT '',`DEP_MGR` VARCHAR(100) DEFAULT '',`DEP_SPV` VARCHAR(100) DEFAULT ''," +
                                    "`ADDID` VARCHAR(100) DEFAULT NULL,`ADDTIME` DATETIME DEFAULT NULL," +
                                    "PRIMARY KEY (`DEP_KD`, `DIV_KD`)) ENGINE=INNODB DEFAULT CHARSET=latin1;");

                    // Query Insert `temp_dept_ws`
                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_dept_ws` (`DEP_KD`, `DEP_NA`, `DIV_KD`, `DEP_MGR`, `DEP_SPV`, `ADDID`, `ADDTIME`) VALUES ");

                    foreach (var dept in dtDEPT)
                    {
                        sqlBuild.Append($"('{dept.DEP_KD.Replace("'", "''")}', '{dept.DEP_NA.Replace("'", "''")}', '{dept.DIV_KD.Replace("'", "''")}', " +
                                        $"'{dept.DEP_MGR.Replace("'", "''")}', '{dept.DEP_SPV.Replace("'", "''")}', USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sqlBuild.Append(";");
                    sql = sqlBuild.ToString();
                    sqlBuild.Clear();
                }
                else
                {
                    setIsSend = false;
                }

                return sql;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return "";
            }
        }
        public async Task<string> CreateQueryKATEGORI_ByTGLPROSES(string jobs, string tglProses, bool setIsSend)
        {
            using var dtService = db.CreateConnection();
            var sqlBuild = new StringBuilder();
            string sql = "";

            try
            {
                var dtKATEGORI = (await dtService.QueryAsync<KATEGORI>(
                    $"SELECT * FROM `{db.DatabaseName()}`.`master_all_kategori` " +
                    $"WHERE `TGL_PROSES` = '{tglProses}';")).ToList();

                if (dtKATEGORI.Count > 0)
                {
                    setIsSend = true;

                    // DROP & CREATE Table `temp_kategori_ws`
                    sqlBuild.Append("DROP TABLE IF EXISTS `pos`.`temp_kategori_ws`; " +
                                    "CREATE TABLE IF NOT EXISTS `pos`.`temp_kategori_ws` (" +
                                    "`KAT_KD` VARCHAR(25) DEFAULT '',`KAT_NA` VARCHAR(100) DEFAULT ''," +
                                    "`DEP_KD` VARCHAR(25) DEFAULT '',`DIV_KD` VARCHAR(25) DEFAULT ''," +
                                    "`KAT_MGR` VARCHAR(50) DEFAULT '',`KAT_SPV` VARCHAR(50) DEFAULT ''," +
                                    "`ADDID` VARCHAR(100) DEFAULT NULL,`ADDTIME` DATETIME DEFAULT NULL," +
                                    "PRIMARY KEY (`KAT_KD`, `DEP_KD`, `DIV_KD`)) ENGINE=INNODB DEFAULT CHARSET=latin1;");

                    // Query Insert `temp_kategori_ws`
                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_kategori_ws` (`KAT_KD`, `KAT_NA`, `DEP_KD`, `DIV_KD`, `KAT_MGR`, `KAT_SPV`, `ADDID`, `ADDTIME`) VALUES ");

                    foreach (var kat in dtKATEGORI)
                    {
                        sqlBuild.Append($"('{kat.KAT_KD.Replace("'", "''")}', '{kat.KAT_NA.Replace("'", "''")}', '{kat.DEP_KD.Replace("'", "''")}', " +
                                        $"'{kat.DIV_KD.Replace("'", "''")}', '{kat.KAT_MGR.Replace("'", "''")}', '{kat.KAT_SPV.Replace("'", "''")}', USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sqlBuild.Append(";");
                    sql = sqlBuild.ToString();
                    sqlBuild.Clear();
                }
                else
                {
                    setIsSend = false;
                }

                return sql;
            }
            catch (Exception ex)
            {
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return "";
            }
        }
        public string CreateQueryTATPrd_Toko(string jobs, List<TAT_PRD> listTAT, bool setIsSend)
        {
            var sqlBuild = new StringBuilder();
            string sql = "";

            try
            {
                if (listTAT.Count > 0)
                {
                    setIsSend = true;

                    // CREATE Table `temp_tatmast_prd`
                    sqlBuild.Append("CREATE TABLE IF NOT EXISTS `pos`.`temp_tatmast_prd` (" +
                                    "`TAT_ID` VARCHAR(90) DEFAULT '',`PRDCD` VARCHAR(24) DEFAULT '',`TOKO_IN` TEXT," +
                                    "`ADDID` VARCHAR(100) DEFAULT NULL,`ADDTIME` DATETIME DEFAULT NULL," +
                                    "`UPDID` VARCHAR(100) DEFAULT NULL,`UPDTIME` DATETIME DEFAULT NULL," +
                                    "PRIMARY KEY (`TAT_ID`, `PRDCD`)) ENGINE=INNODB DEFAULT CHARSET=latin1;");

                    // Query Insert `temp_tatmast_prd`
                    sqlBuild.Append("INSERT IGNORE INTO `pos`.`temp_tatmast_prd` (`TAT_ID`, `PRDCD`, `TOKO_IN`, `ADDID`, `ADDTIME`, `UPDID`, `UPDTIME`) VALUES ");

                    foreach (var tat in listTAT)
                    {
                        sqlBuild.Append($"('{tat.TAT_ID.Replace("'", "''")}', '{tat.PRDCD.Replace("'", "''")}', '{tat.TOKO_IN.Replace("'", "''")}', " +
                                        $"USER(), NOW(), USER(), NOW()),");
                    }

                    sqlBuild.Length--;
                    sqlBuild.Append(" ON DUPLICATE KEY UPDATE `TOKO_IN`=VALUES(`TOKO_IN`), `UPDID`=USER(), `UPDTIME`=NOW();");
                    sql = sqlBuild.ToString();
                    sqlBuild.Clear();
                }
                else
                {
                    setIsSend = false;
                }

                return sql;
            }
            catch (Exception ex)
            {
                setIsSend = false;
                _objUtil.Tracelog(jobs, ex.Message + "\r\n" + ex.StackTrace, Utility.TipeLog.Error);
                return "";
            }
        }
    }
}
