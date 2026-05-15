using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class ResponseRestApi
    {
        public int status { get; set; }
        public string? message { get; set; }
        public string? data { get; set; }
    }
}
