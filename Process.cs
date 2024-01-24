using System;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Threading;
using System.Data;
using System.Linq;
using System.IO;
using System.Configuration;
using Microsoft.Extensions.Logging;

namespace SiknSqlRepeater
{
    class Process
    {
        object obj = new object();
        bool enabled = true;
        private string siknConnString;
        private string coreConnString;
        private string from_tables;
        private string to_tables;
        private string logFile;
        private string siknDbName;
        private string CoreDbName;
        private int CountCheckRecord;
        private int ScanPeriod_Ms;
        ILoggerFactory factory;
        ILogger winLogger;

        public Process()
        {
            try
            {

                this.siknConnString = ConfigurationManager.AppSettings["siknConnString"];
                this.coreConnString = ConfigurationManager.AppSettings["coreConnString"];
                this.from_tables = ConfigurationManager.AppSettings["from_tables"];
                this.to_tables = ConfigurationManager.AppSettings["to_tables"];
                if (this.from_tables.Split(',').Count() == 0)
                {
                    throw new InvalidDataException("Неверно указаны таблицы для сравнения. Укажите названия без пробелов через запятую");
                }
                if (this.to_tables.Split(',').Count() == 0)
                {
                    throw new InvalidDataException("Неверно указаны таблицы для сравнения. Укажите названия без пробелов через запятую");
                }
                this.logFile = ConfigurationManager.AppSettings["logFile"];
                this.siknDbName = ConfigurationManager.AppSettings["siknDbName"];
                this.CoreDbName = ConfigurationManager.AppSettings["CoreDbName"];
                this.CountCheckRecord = Int32.Parse(ConfigurationManager.AppSettings["CountCheckRecord"]);
                this.ScanPeriod_Ms = Int32.Parse(ConfigurationManager.AppSettings["ScanPeriod_Ms"]);
                this.factory = LoggerFactory.Create(builder => builder.AddEventLog(eventLogSettings => eventLogSettings.SourceName = "SiknToHnp"));
                this.winLogger = factory.CreateLogger<Program>();


            }
            catch (Exception e)
            {
                winLogger.LogError("Ошибка чтения параметров, ошибка: " + e);
            }
            finally
            {
                winLogger.LogInformation("Чтение параметров успешно");
            }
        }

        public void Start()
        {
            while (enabled)
            {
                RunProcess();
                Thread.Sleep(ScanPeriod_Ms);
            }

        }
        public void Stop()
        {
            enabled = false;
        }

        private void RecordEntry(string message)
        {


            lock (obj)
            {
                using (StreamWriter writer = new StreamWriter(logFile, true))
                {
                    writer.WriteLine(String.Format("{0} - {1}",
                        DateTime.Now.ToString("dd/MM/yyyy hh:mm:ss"), message));
                    writer.Flush();
                }
            }
        }

        private bool ProcessForTable(string fromTableName, string toTableName)
        {
            string sql = $"SELECT TOP(10) [{siknDbName}].[dbo].[{fromTableName}].* " +
                         $", [{siknDbName}].[dbo].[DocPassport].iPassNumber " +
                         $"FROM [{siknDbName}].[dbo].[{fromTableName}] " +
                         $"LEFT JOIN [{siknDbName}].[dbo].[DocPassport] " +
                         $"ON [{siknDbName}].[dbo].[{fromTableName}].dDateBegin = [{siknDbName}].[dbo].[DocPassport].dDateBegin " +
                         $"AND [{siknDbName}].[dbo].[{fromTableName}].dDateEnd = [{siknDbName}].[dbo].[DocPassport].dDateEnd " +
                         $"AND [{siknDbName}].[dbo].[{fromTableName}].iTypePeriod = [{siknDbName}].[dbo].[DocPassport].iTypePeriod " +
                         $"ORDER BY [{siknDbName}].[dbo].[{fromTableName}].ID desc";
            DBQuery DB = new DBQuery();
            DataTable sikn_acts = DB.QuerySelect(siknConnString, sql);
            if (sikn_acts.Rows.Count == 0)
            {
                winLogger.LogError($"Ошибка чтения из таблицы: {fromTableName}");
                throw new InvalidDataException($"Ошибка чтения из таблицы: {fromTableName}, БД: СИКН");
            }

            sql = $"SELECT TOP {CountCheckRecord} * FROM [{CoreDbName}].[dbo].[{toTableName}] ORDER BY ID DESC";
            DataTable core_acts = DB.QuerySelect(coreConnString, sql);

            string[] core_act_id = new string[core_acts.Rows.Count];

            //Создаем массив ID c БД CORE
            int index = 0;
            if (core_acts.Rows.Count != 0)
            {
                foreach (DataRow row in core_acts.Rows)
                {
                    if (row.ItemArray.Count() == 0)
                    {
                        winLogger.LogError($"В записи нет данных: {toTableName}, БД: EIS");
                        throw new InvalidDataException($"В записи нет данных: {toTableName}, БД: EIS");
                    }
                    var cells = row.ItemArray;
                    core_act_id[index] = cells[0].ToString();
                    index++;
                }
            }
            string[] act_id_not_contains = new string[sikn_acts.Rows.Count];
            List<DataRow> not_contains_record = new List<DataRow>();

            //Перебираем записи с СИКН и проверяем есть ли ID записи с СИКН в массиве IDшников c БД CORE
            index = 0;
            foreach (DataRow row in sikn_acts.Rows)
            {
                var cells2 = row.ItemArray;
                if (!(core_act_id.Contains(cells2[0].ToString())))
                {
                    not_contains_record.Add(row);
                    index++;
                }
            }
            string insert_sql = $"SELECT TOP {CountCheckRecord} * FROM [{CoreDbName}].[dbo].[{toTableName}] ORDER BY ID DESC";
            if (not_contains_record.Count != 0)
            {
                string id = "";
                if (not_contains_record.Count > 1)
                {
                    foreach (DataRow record in not_contains_record)
                    {
                        var cells = record.ItemArray;
                        id = id + "," + cells[0].ToString();
                    }
                }
                else
                {
                    var cells = not_contains_record[0];
                    id = id + "," + cells[0].ToString();
                }
                id = id.Substring(1);
                winLogger.LogInformation($"Добавляю запись c ID - {id} в таблицу - {toTableName}");
                DBQuery DB_insert = new DBQuery();
                DB_insert.QueryInsert(coreConnString, insert_sql, not_contains_record);
                core_acts.Clear();

                foreach (DataRow record in not_contains_record)
                {
                    var id_cells = record.ItemArray[0];
                    sql = $"SELECT * FROM [{CoreDbName}].[dbo].[{toTableName}] WHERE ID={id_cells}";
                    core_acts = DB.QuerySelect(coreConnString, sql);
                    if (core_acts.Rows.Count == 0)
                    {
                        winLogger.LogError($"Запись с ID - {id_cells} не добавлена в БД: EIS, таблица: {toTableName}");
                        throw new InvalidDataException($"Запись с ID - {id_cells} не добавлена в БД: EIS, таблица: {toTableName}");
                    }
                    winLogger.LogInformation($"Запись с ID - {id_cells} успешно добавлена в БД EIS, таблица {toTableName}");
                }
            }
            return true;
        }

        private void RunProcess()
        {
            try
            {
                int tableIndex = 0;
                foreach (string fromTable in this.from_tables.Split(','))
                {
                    string toTable = this.to_tables.Split(',')[tableIndex];
                    ProcessForTable(fromTable, toTable);
                    tableIndex++;
                }
            }
            catch (Exception e)
            {
                winLogger.LogError("Error: " + e);
                RecordEntry("Error: " + e);
                Console.WriteLine("Error: " + e);
                Console.WriteLine(e.StackTrace);
            }
        }
    }
}
