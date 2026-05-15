using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class PROMO
    {
        public string KODEPROMO { get; set; } = "";
        public string SUBKODEPROMO { get; set; } = "";
        public string TIPEPROMO { get; set; } = "";
        public string KODEGROUP { get; set; } = "";
        public string MEKANISME { get; set; } = "";
        public string CETAKSTRUK1 { get; set; } = "";
        public string CETAKSTRUK2 { get; set; } = "";
        public string CETAKSTRUK3 { get; set; } = "";
        public string CETAKLAYAR1 { get; set; } = "";
        public string CETAKLAYAR2 { get; set; } = "";
        public string CETAKLAYAR3 { get; set; } = "";
        public string TANGGALAWAL { get; set; } = "";
        public string TANGGALAKHIR { get; set; } = "";
        public string PERIODEJAM { get; set; } = "";
        public string PERIODEMINGGUAN { get; set; } = "";
        public string PERIODEBULANAN { get; set; } = "";
        public string ITEMSYARAT { get; set; } = "";
        public string QTYSYARATMIN { get; set; } = "";
        public string QTYSYARATMAX { get; set; } = "";
        public string QTYSYARATTAMBAH { get; set; } = "";
        public string RPSYARATMIN { get; set; } = "";
        public string RPSYARATMAX { get; set; } = "";
        public string RPSYARATTAMBAH { get; set; } = "";
        public string ITEMTARGET { get; set; } = "";
        public string QTYTARGET { get; set; } = "";
        public string QTYTARGETMAX { get; set; } = "";
        public string QTYTARGETTAMBAH { get; set; } = "";
        public string RPTARGET { get; set; } = "";
        public string RPTARGETMAX { get; set; } = "";
        public string RPTARGETTAMBAH { get; set; } = "";
        public string POTONGANPERSENTARGET { get; set; } = "";
        public string KODEBIN { get; set; } = "";
        public string KODEMEMBER { get; set; } = "";
        public string BERSYARAT { get; set; } = "";
        public string KODESHIFT { get; set; } = "";
        public string NOSTRUK { get; set; } = "";
        public string SYARATCAMPUR { get; set; } = "";
        public string TRANSAKSIMAX { get; set; } = "";
        public string QTYTARGETMAXPROMO { get; set; } = "";
        public string RPTARGETMAXPROMO { get; set; } = "";
        public string TARGETTDKDIJUAL { get; set; } = "";
        public int NOURUT { get; set; }
        public string STICKER { get; set; } = "";
        public string PEMBATASAN_HADIAH { get; set; } = "";
        public string TENGGANGWAKTU { get; set; } = "";
        public string QTYMAXPERSTRUK { get; set; } = "";
        public string FLHIMPACT { get; set; } = "";
        public string POTONGMARGIN { get; set; } = "";
        public string MAXRPBELANJA { get; set; } = "";
        public string TGL_UPDATE { get; set; } = "";
        public string QTY_TBS_MRH { get; set; } = "";
        public string KEYPROMOSI { get; set; } = "";
        public string IKIOS { get; set; } = "";
        public string PRIORITAS { get; set; } = "";
        public string ID { get; set; } = "";
        public string I_STORE { get; set; } = "";
        public string SPECIAL_PRODUCT { get; set; } = "";
        public string PRO_SPECIAL_REQUEST { get; set; } = "";
        public string PRO_JENIS_SPECIAL_REQ { get; set; } = "";
        // Revisi (12 April 2022) : Tambah field PRO_EXCLUDE_BIN
        public string PRO_EXCLUDE_BIN { get; set; } = "";
        // Revisi (26 Januari 2024) : Tambah field HARIED - Memo 1800/CPS/23
        public string HARIED { get; set; } = "";
        // Revisi (15 Juli 2024) : Tambah field PRO_DESC_PRICE_TAG - Project Deskripsi PriceTag (Email Pak Andry 12 Juli 2024)
        public string PRO_DESC_PRICE_TAG { get; set; } = "";
        // Revisi (12 Agustus 2024) : Tambah field PRO_JENIS_SHELFTALKER - Project Jenis Shelftalker (Memo 1237/CPS/24 - 1487/09-24/E/PMO)
        public string PRO_JENIS_SHELFTALKER { get; set; } = "";
    }
}
