using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrPrCabang_NetCore.Services
{
    public class PosRtSoapService
    {
        private readonly HttpClient _httpClient;
        private readonly Utility _objUtil;

        public PosRtSoapService(HttpClient httpClient, Utility objUtil)
        {
            _httpClient = httpClient;
            _objUtil = objUtil;
        }

        private async Task<string> SendSoapRequest(string url, string soapAction, string soapBody)
        {
            var soapEnvelope =
                $"""
            <?xml version="1.0" encoding="utf-8"?>
            <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                           xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                           xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                <soap:Body>
                    {soapBody}
                </soap:Body>
            </soap:Envelope>
            """;

            var request = new HttpRequestMessage(HttpMethod.Post, url)
            {
                Content = new StringContent(soapEnvelope, Encoding.UTF8, "text/xml")
            };
            request.Headers.Add("SOAPAction", $"\"{soapAction}\"");

            var response = await _httpClient.SendAsync(request);
            return await response.Content.ReadAsStringAsync();
        }

        private static string ParseSoapResult(string responseXml, string methodName)
        {
            var doc = new System.Xml.XmlDocument();
            doc.LoadXml(responseXml);

            var nsManager = new System.Xml.XmlNamespaceManager(doc.NameTable);
            nsManager.AddNamespace("soap", "http://schemas.xmlsoap.org/soap/envelope/");
            nsManager.AddNamespace("tns", "http://tempuri.org/");

            var resultNode = doc.SelectSingleNode($"//tns:{methodName}Result", nsManager);
            return resultNode?.InnerText ?? "";
        }

        public async Task<string> InsertTrPrCabAsync(string url, string judulTask, string toko, byte[] sqlCmd)
        {
            var soapBody =
                $"""
            <InsertTrPrCab xmlns="http://tempuri.org/">
                <judulTask>{judulTask}</judulTask>
                <toko>{toko}</toko>
                <sqlCmd>{Convert.ToBase64String(sqlCmd)}</sqlCmd>
            </InsertTrPrCab>
            """;

            var responseXml = await SendSoapRequest(url, "http://tempuri.org/InsertTrPrCab", soapBody);
            return ParseSoapResult(responseXml, "InsertTrPrCab");
        }

        public async Task<string> ExecuteQueryAsync(string url, byte[] sqlCmd)
        {
            var soapBody =
                $"""
            <ExecuteQuery xmlns="http://tempuri.org/">
                <sqlCmd>{Convert.ToBase64String(sqlCmd)}</sqlCmd>
            </ExecuteQuery>
            """;

            var responseXml = await SendSoapRequest(url, "http://tempuri.org/ExecuteQuery", soapBody);
            return ParseSoapResult(responseXml, "ExecuteQuery");
        }
    }
}
