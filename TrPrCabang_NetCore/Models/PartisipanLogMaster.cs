using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class LogMasterTKXDCX
    {
        public string? KODETOKO { get; set; }
        public string? TGL_PROSES { get; set; }
        public string? TGL_STATUS { get; set; }
        public string? JENIS_TKX { get; set; }
        public string? JENIS_DCX { get; set; }
    }

    public class LogMasterDIVDEPTKATE
    {
        public string? KODETOKO { get; set; }
        public string? TGL_PROSES { get; set; }
        public string? TGL_STATUS { get; set; }
        public string? JENIS_DIV { get; set; }
        public string? JENIS_DEPT { get; set; }
        public string? JENIS_KATE { get; set; }
    }

    public class LogMasterBase
    {
        public string KODETOKO { get; set; }
        public string TGL_STATUS { get; set; }
        public string TGL_PROSES { get; set; }
    }
}
