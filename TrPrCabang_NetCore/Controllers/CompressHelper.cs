using Microsoft.IdentityModel.Tokens;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Controllers
{
    public class CompressHelper
    {
        private readonly Utility ObjUtil;

        public CompressHelper(Utility objUtil)
        {
            ObjUtil = objUtil;
        }

        public byte[] Compress(string varStr)
        {
            try
            {
                byte[] tempByte = Encoding.UTF8.GetBytes(varStr);
                using var memory = new MemoryStream();
                using (var gzip = new GZipStream(memory, CompressionMode.Compress, true))
                {
                    gzip.Write(tempByte, 0, tempByte.Length);
                }
                return memory.ToArray();
            }
            catch
            {
                return null;
            }
        }

        public string Decompress(string jobs, byte[] byteData)
        {
            try
            {
                using var stream = new GZipStream(new MemoryStream(byteData), CompressionMode.Decompress);
                const int size = 4096;
                var buffer = new byte[size];
                using var memoryStream = new MemoryStream();
                int count;
                do
                {
                    count = stream.Read(buffer, 0, size);
                    if (count > 0)
                        memoryStream.Write(buffer, 0, count);
                } while (count > 0);

                return Encoding.UTF8.GetString(memoryStream.ToArray());
            }
            catch
            {
                ObjUtil.Tracelog(jobs, "ERROR-Decompress: Format Data API [Byte] Invalid", Utility.TipeLog.Error);
                return "ERROR - Format Data API [Byte] Invalid";
            }
        }

        public string Decrypt(string jobs, string value, MySqlConnection connCabang)
        {
            try
            {
                byte[] byteData = Convert.FromBase64String(value.Replace("\"", ""));
                using var stream = new GZipStream(new MemoryStream(byteData), CompressionMode.Decompress);
                const int size = 4096;
                var buffer = new byte[size];
                using var memoryStream = new MemoryStream();
                int count;
                do
                {
                    count = stream.Read(buffer, 0, size);
                    if (count > 0)
                        memoryStream.Write(buffer, 0, count);
                } while (count > 0);

                return Encoding.UTF8.GetString(memoryStream.ToArray());
            }
            catch (Exception ex)
            {
                ObjUtil.Tracelog(jobs, "ERROR-Decompress: Format Data API [Byte] Invalid", Utility.TipeLog.Error);
                return $"ERROR : {ex.Message}";
            }
        }

        public string CompressAndEncodeString(string jString)
        {
            try
            {
                byte[] tempByte = Encoding.UTF8.GetBytes(jString);
                using var memory = new MemoryStream();
                using (var gzip = new GZipStream(memory, CompressionMode.Compress, true))
                {
                    gzip.Write(tempByte, 0, tempByte.Length);
                }
                return Convert.ToBase64String(memory.ToArray());
            }
            catch
            {
                return null;
            }
        }
    }
}
