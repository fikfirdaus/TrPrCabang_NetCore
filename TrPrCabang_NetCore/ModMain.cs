using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore
{
    public class ModMain
    {
        private readonly Utility _objUtil;

        public async Task RunAsync()
        {
            try
            {
                string[] args = Environment.GetCommandLineArgs();
                string arg = args.Length > 1 ? args[1].Trim().ToUpper() : string.Empty;

                switch (arg)
                {
                    case "G":
                    case "S":
                    case "G2":
                        new FrmMain();
                        break;
                    case "S2":
                        // new FrmSend2().Run();
                        break;
                    case "TOK":
                        // new FrmToko().Run();
                        break;
                    default:
                        // new FrmMain().Run();
                        break;
                }
            }
            catch (Exception ex)
            {
                _objUtil.TraceLogFile($"Error ModMain : {ex.Message}\r\n{ex.StackTrace}");
                Environment.Exit(1);
            }
        }
    }
}
