using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Models
{
    public class Config
    {
        public string? KodeCabang { get; set; }
        public string? IpClient { get; set; }
        public int MaxThread { get; set; }
        public int TimeoutService { get; set; }

        public string? UrlStatusPajak { get; set; }
        public string? UrlMasterPajak { get; set; }
        public string? UrlStatusSupplier { get; set; }
        public string? UrlMasterSupplier { get; set; }
        public string? UrlPartisipanSupplier { get; set; }
        public string? UrlStatusPromo { get; set; }
        public string? UrlMasterPromo { get; set; }
        public string? UrlPartisipanPromo { get; set; }

        public string? DeliStatusPajak { get; set; }
        public string? DeliMasterPajak { get; set; }
        public string? DeliStatusSupplier { get; set; }
        public string? DeliMasterSupplier { get; set; }
        public string? DeliPartisipanSupplier { get; set; }
        public string? DeliStatusPromo { get; set; }
        public string? DeliMasterPromo { get; set; }
        public string? DeliPartisipanPromo { get; set; }

        public string? UrlPosRT_Service { get; set; }
        public string? UrlPosRT_RestApi { get; set; }
        public bool IsPosRT_RestApi { get; set; }
        public string? AuthUserPosRT_RestApi { get; set; }
        public string? AuthPassPosRT_RestApi { get; set; }

        public string? UrlStatusProduk { get; set; }
        public string? UrlMasterProduk { get; set; }

        public string? UrlMasterTKX { get; set; }
        public string? UrlStatusTKX { get; set; }
        public string? UrlMasterDCX { get; set; }
        public string? UrlStatusDCX { get; set; }

        public string? UrlMasterDIVISI { get; set; }
        public string? UrlStatusDIVISI { get; set; }
        public string? UrlMasterDEPT { get; set; }
        public string? UrlStatusDEPT { get; set; }
        public string? UrlMasterKATEGORI { get; set; }
        public string? UrlStatusKATEGORI { get; set; }

        public string? UrlStatusSaranaProduk { get; set; }
        public string? UrlMasterSaranaProduk { get; set; }

        public string? UrlStatusPluKrat { get; set; }
        public string? UrlMasterPluKrat { get; set; }

        public string? UrlStatusTATPrd { get; set; }
        public string? UrlMasterTATPrd { get; set; }

        public string? UrlApiPersediaanToko { get; set; }
        public string? UsernameApiPersediaanToko { get; set; }
        public string? PasswordApiPersediaanToko { get; set; }
        public bool SendMastProdSus { get; set; }

        public string? UrlStatusTTT { get; set; }
        public string? UrlMasterTTT { get; set; }
        public string? UrlStatusTAT { get; set; }
        public string? UrlMasterTAT { get; set; }

        public string? UrlStatusTokoProdSus { get; set; }
        public string? UrlMasterTokoProdSus { get; set; }
        public string? UrlStatusProdSusJualToko { get; set; }
        public string? UrlMasterProdSusJualToko { get; set; }
        public string? UrlStatusTokoProdSusPeriode { get; set; }
        public string? UrlMasterTokoProdSusPeriode { get; set; }

    }
}
