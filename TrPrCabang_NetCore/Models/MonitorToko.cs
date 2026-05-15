using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class ClsRequest
    {
        public string? JENIS_MASTER { get; set; }
        public string? USER { get; set; }
        public string? TIME_STAMP { get; set; }
    }

    public class ClsRespon
    {
        public string? ERR_CODE { get; set; }
        public string? ERR_MSG { get; set; }
        public object? DETAIL { get; set; }
    }

    public class TOKO_EXTENDED
    {
        public string Tok_ID { get; set; } = "";
        public string KODE_TOKO { get; set; } = "";
        public string NAMA_TOKO { get; set; } = "";
        public string KODE_GUDANG { get; set; } = "";
        public string TGL_BUKA { get; set; } = "";
    }
}
