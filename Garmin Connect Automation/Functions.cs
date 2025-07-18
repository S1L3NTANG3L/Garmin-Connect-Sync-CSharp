using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System;
using System.Data;
using System.IO;
using System.Globalization;

namespace Garmin_Connect_Automation
{
    public class Functions
    {
        public static DataTable ReadCsv(string filePath)
        {
            var dataTable = new DataTable();
            using (var reader = new StreamReader(filePath))
            {
                var headers = reader.ReadLine()?.Split(';');
                if (headers == null)
                {
                    throw new Exception("CSV file is empty or malformed");
                }
                // Add columns based on the headers
                foreach (var header in headers)
                {
                    dataTable.Columns.Add(header);
                }
                // Read data rows
                while (!reader.EndOfStream)
                {
                    var row = reader.ReadLine()?.Split(';');

                    if (row != null)
                    {
                        dataTable.Rows.Add(row);
                    }
                }
            }
            return dataTable;
        }

        public static DataTable FilterDataTable(DataTable dtIn, int intDays)
        {
            var filteredRows = dtIn.AsEnumerable()
                .Where(row =>
                {
                    var timestamp = DateTime.ParseExact(
                        row["timestamp"].ToString().Trim(),
                        "yyyy-MM-dd HH:mm:ss.ffffff",
                        CultureInfo.InvariantCulture
                    );
                    return timestamp >= DateTime.Today.AddDays(intDays);
                });
            // If no rows match the condition, return an empty DataTable
            if (!filteredRows.Any()) // Use .Any() to check if the collection is empty
            {
                return dtIn.Clone(); // Clone gives an empty DataTable with the same schema
            }
            // Otherwise, convert the filtered rows back to a DataTable
            return filteredRows.CopyToDataTable();
        }
        public static DataTable FilterRowsByDay(DataTable dtIn, int intDays)
        {
            var filteredRows = dtIn.AsEnumerable()
                .Where(row => DateTime.ParseExact(
                   row["day"].ToString().Trim(),
                   "yyyy-MM-dd",
                   CultureInfo.InvariantCulture) 
                   >= DateTime.Today.AddDays(intDays));
            // If no rows match, return an empty DataTable with the same schema
            if (!filteredRows.Any())
            {
                return dtIn.Clone(); // Clone creates an empty DataTable with the same schema as dtIn
            }
            return filteredRows.CopyToDataTable();
        }
        public static void WriteDataTableToCSV(DataTable dataTable, string filePath)
        {
            using (var writer = new StreamWriter(filePath))
            {
                // Write the header row
                var header = string.Join(";", dataTable.Columns.Cast<DataColumn>().Select(c => c.ColumnName));
                writer.WriteLine(header);

                // Write the data rows
                foreach (DataRow row in dataTable.Rows)
                {
                    var line = string.Join(";", row.ItemArray);
                    writer.WriteLine(line);
                }
            }
        }
    }
}
