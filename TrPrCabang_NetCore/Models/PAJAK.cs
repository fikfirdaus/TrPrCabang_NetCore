using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class PAJAK
    {
        public string KD_DC { get; set; } = "";
        public string TOK_CODE { get; set; } = "";
        public string TOK_SKP { get; set; } = "";
        public string TOK_NAMA_FRC { get; set; } = "";
        public string TOK_NPWP { get; set; } = "";
        public string START_TGL_BERLAKU_SE { get; set; } = "";
        public string END_TGL_BERLAKU_SE { get; set; } = "";
        public string FLAG_PKP { get; set; } = "";
    }
}
