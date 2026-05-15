using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class PERSEDIAAN_TOKO
    {
        public int status { get; set; }
        public string? message { get; set; }
        public PERSEDIAAN_TOKO_DATA? data { get; set; }
    }

    public class PERSEDIAAN_TOKO_DATA
    {
        public string? guid { get; set; }
    }
}
