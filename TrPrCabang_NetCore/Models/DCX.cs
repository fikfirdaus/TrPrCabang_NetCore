using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class DCX
    {
        public string KODE_TOKO { get; set; } = "";
        public string NAMA_TOKO { get; set; } = "";
        public string KODE_DC { get; set; } = "";
        public string NAMA_DC { get; set; } = "";
        public string KODE_CABANG { get; set; } = "";
        public string NAMA_CABANG { get; set; } = "";
        public string KODE_CABANG_OLD { get; set; } = "";
        public string NAMA_CABANG_OLD { get; set; } = "";
        public string TYPE_DC { get; set; } = "";
        public string TGL_AWAL { get; set; } = "";
        public string TGL_AKHIR { get; set; } = "";
        public string MARK_UP_IGR { get; set; } = "";
        // Revisi (11 April 2022) : Tambah property FLAGPROD
        public string FLAGPROD { get; set; } = "";
    }
}
