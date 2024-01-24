using System;
using System.Data.SqlClient;
using System.Data.Common;
using System.IO;
using System.Data;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using System.Configuration;

namespace SiknSqlRepeater
{
    class DBQuery
    {
        ILoggerFactory factory;
        ILogger winLogger;

        public DBQuery()
        {
            this.factory = LoggerFactory.Create(builder => builder.AddEventLog(eventLogSettings => eventLogSettings.SourceName = "SIKN_TO_HNP"));
            this.winLogger = factory.CreateLogger<Program>();
        }
        public bool QueryInsert(string connectionString, string sql, List<DataRow> data)
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);
                DataSet ds = new DataSet();
                adapter.Fill(ds);
                //if (ds.Tables.Count != 0)
                //{
                DataTable dt = ds.Tables[0];

                int index;
                foreach (DataRow row in data)
                {
                    index = 0;
                    DataRow newRow = dt.NewRow();
                    foreach (var cell in row.ItemArray)
                    {

                        newRow[index] = cell;
                        index++;
                    }
                    dt.Rows.Add(newRow);
                }
                SqlCommandBuilder comanndBuilder = new SqlCommandBuilder(adapter);
                adapter.Update(dt);
                return true;

            }
        }


        public DataTable QuerySelect(string connectionString, string sql)
        {


            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                try
                {
                    SqlDataAdapter adapter = new SqlDataAdapter(sql, connection);

                    DataSet ds = new DataSet();
                    adapter.Fill(ds);
                    if (connectionString == ConfigurationManager.AppSettings["coreConnString"])
                    {
                        return ds.Tables[0];
                    }
                    if (ds.Tables.Count != 0)
                    {
                        return ds.Tables[0];
                    }
                    else
                    {
                        throw new InvalidOperationException("Запрос не вернул данные");
                    }

                }
                catch (Exception e)
                {
                    winLogger.LogError($"Ошибка чтения из таблицы. Строка подключения: {connectionString}. Запрос: {sql}, ошибка: " + e);
                    return new DataTable();
                }

            }
        }

    }
}