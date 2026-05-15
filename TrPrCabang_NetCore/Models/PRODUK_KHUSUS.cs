using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class PRODUK_KHUSUS
    {
        public string KODE_MODUL { get; set; } = "";
        public string NAMA_MODUL { get; set; } = "";
        public string FLAG_SO { get; set; } = "";
        public string JADWAL_HARI_SO { get; set; } = "";
        public string PLU { get; set; } = "";
        public string RASIO_BELI { get; set; } = "";
        public string SALES_GROWTH { get; set; } = "";
        public string KELIPATAN_PRODUKSI { get; set; } = "";
    }
}
