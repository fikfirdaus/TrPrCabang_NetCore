using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class TOKO_PRODSUS
    {
        public string? KD_CABANG { get; set; }
        public string? NM_CABANG { get; set; }
        public string? KD_TOKO { get; set; }
        public string? NM_TOKO { get; set; }
        public string? FLAG_TOKO { get; set; }
        public string? CK_KIRIM { get; set; }
    }

    public class PRODSUS_JUAL_TOKO
    {
        public string? KD_TOKO { get; set; }
        public string? NM_TOKO { get; set; }
        public string? KD_CABANG { get; set; }
        public string? MERK { get; set; }
        public string? PLU { get; set; }
        public string? NM_PRODUK { get; set; }
        public string? TGL_AKTIF_MERK { get; set; }
    }

    public class TOKO_PRODSUS_PERIODE
    {
        public string? KD_TOKO { get; set; }
        public string? NM_TOKO { get; set; }
        public string? KD_CABANG { get; set; }
        public string? HARI { get; set; }
        public string? PERIODE_JUAL { get; set; }
        public string? JAM_AWAL { get; set; }
        public string? JAM_AKHIR { get; set; }
    }

}
