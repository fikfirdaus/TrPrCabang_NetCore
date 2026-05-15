using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class PARTISIPAN_PROMO
    {
        public string KD_DC { get; set; } = "";
        public string KD_TOKO { get; set; } = "";
        public string KD_PROMO { get; set; } = "";
        public int NO_URUT { get; set; }
        public string TGL_UPDATE { get; set; } = "";
    }

    public class PARTISIPAN_SUPPLIER
    {
        public string KDCAB { get; set; } = "";
        public string KDTK { get; set; } = "";
        public string SUPCO { get; set; } = "";
        public int LT { get; set; }
        public string JADWAL { get; set; } = "";
        public string DATANG { get; set; } = "";
        public int KAPASITAS { get; set; }
        public string FAX { get; set; } = "";
    }
}
