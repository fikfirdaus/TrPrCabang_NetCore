using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Mail;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;
using TrPrCabang_NetCore.Connection;
using Dapper;

namespace TrPrCabang_NetCore
{
    public class Utility
    {
        private readonly IDbServices db;

        public Utility(IDbServices db)
        {
            this.db = db;
        }

        public enum TipeLog
        {
            Debug,
            Error,
            Info,
            Warning
        }

        public void Tracelog(string jobs, string log, TipeLog tipe)
        {
            using var dtService = db.CreateConnection();
            string sql = string.Empty;
            string appVersion = string.Empty;
            // string appVersion = $"{Application.ProductName.Trim()} v{Application.ProductVersion.Trim()}";
            try
            {
                string safeLog = log.Replace("'", "`");

                if (jobs.ToUpper().Contains("GET"))
                {
                    sql = $"INSERT INTO `{db.DatabaseName()}`.`tracelog_get` (Tgl,Tipe,Appname,Log,AddId)" +
                          $" VALUES (NOW(), '{jobs}', '{appVersion}', '{safeLog}', USER());";
                }
                else if (jobs.ToUpper().Contains("SEND"))
                {
                    sql = $"INSERT INTO `{db.DatabaseName()}`.`tracelog_send` (Tgl,Tipe,Appname,Log,AddId)" +
                          $" VALUES (NOW(), '{jobs}', '{appVersion}', '{safeLog}', USER());";
                }

                if (sql.Trim().Length > 0)
                {
                    using var conn = db.CreateConnection();
                    conn.Execute(sql);
                }
                else
                {
                    using var sw = new StreamWriter(Path.Combine(AppContext.BaseDirectory, "trpr.log"), true);
                    sw.WriteLine($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: \r\n{log.Replace("'", "`")}");
                    sw.Flush();
                }
            }
            catch (Exception ex)
            {
                try
                {
                    using var sw = new StreamWriter(Path.Combine(AppContext.BaseDirectory, "trpr.log"), true);
                    sw.WriteLine($"{DateTime.Now:dd-MM-yyyy HH:mm:ss}: {ex.Message}\r\n{ex.StackTrace}\r\nLOG : {log.Replace("'", "`")}");
                    sw.Flush();
                }
                catch { }
            }
        }

        public void TraceLogFile(string log)
        {
            try
            {
                string pathFolder = Path.Combine(AppContext.BaseDirectory, "TRACELOG");
                if (!Directory.Exists(pathFolder))
                    Directory.CreateDirectory(pathFolder);

                using var sw = new StreamWriter($@"{pathFolder}\{DateTime.Now:yyMMdd}.log", true);
                sw.WriteLine(log + "\r\n");
                sw.Flush();
            }
            catch { }
        }

        public void TraceLogJSON(string json)
        {
            try
            {
                string pathFolder = Path.Combine(AppContext.BaseDirectory, "TRACELOG");
                if (!Directory.Exists(pathFolder))
                    Directory.CreateDirectory(pathFolder);

                using var sw = new StreamWriter($@"{pathFolder}\json_{DateTime.Now:yyMMdd}.log", true);
                sw.WriteLine(json + "\r\n");
                sw.Flush();
            }
            catch { }
        }

        public bool SendEmail(string emailTo, string emailCC, string subject, string msg)
        {
            try
            {
                string emailFrom = null;
                string emailServer = null;
                int emailPort = 0;
                string emailUserName = null;
                string emailPassword = null;

                using var mail = new MailMessage();
                mail.From = new MailAddress(emailFrom);
                mail.Subject = subject;
                mail.Priority = MailPriority.High;
                mail.Body = msg;

                if (emailTo != null)
                {
                    foreach (var address in emailTo.Split(';'))
                        if (!string.IsNullOrWhiteSpace(address))
                            mail.To.Add(address);
                }
                else
                {
                    mail.To.Add("wisanggeni@indomaret.co.id");
                }

                if (emailCC != null)
                {
                    foreach (var address in emailCC.Split(';'))
                        if (!string.IsNullOrWhiteSpace(address))
                            mail.CC.Add(address);
                }
                else
                {
                    mail.CC.Add("wisanggeni@indomaret.co.id");
                }

                using var smtpServer = new SmtpClient(emailServer)
                {
                    Port = emailPort,
                    Credentials = new NetworkCredential(emailUserName, emailPassword)
                };
                smtpServer.Send(mail);
                System.Threading.Thread.Sleep(5000);

                return true;
            }
            catch (Exception ex)
            {
                TraceLogFile($"{ex.Message}\r\n{ex.StackTrace}");
                return false;
            }
        }
    }
}
