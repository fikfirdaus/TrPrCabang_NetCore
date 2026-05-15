using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class STATUS
    {
        public string JENIS { get; set; } = "";
        public string CABANG { get; set; } = "";
        public string FLAG_PROSES { get; set; } = "";
        public int JML_RECORD { get; set; }
        public string TGL_TRPR { get; set; } = "";
        public string TGL_STATUS { get; set; } = "";
        public string TGL_PROSES { get; set; } = "";
    }
}
