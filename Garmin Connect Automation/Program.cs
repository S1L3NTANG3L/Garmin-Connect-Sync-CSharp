using System.Data;
using System.Diagnostics;
using static Garmin_Connect_Automation.Functions;
Console.WriteLine("Initialise Variables");
var strDesktopPath = Environment.GetFolderPath(Environment.SpecialFolder.Desktop);
var strLocalAppDataPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
var strUserPath = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
var strPythonPath = strLocalAppDataPath + "\\Programs\\Python\\Python313\\python.exe";
var arrDBNames = new string[] { "garmin_monitoring.db", "garmin.db" };
var strCSVOutputFolderName = "CSVOutputs";
var arrCSVNames = new string[] { "Monitoring",
  "Heart Rate",
  "Respiration Rate",
  "Daily Summary",
  "Resting Heart Rate",
  "Sleep",
  "Stress",
  "Sleep Events"};

var arrCSVFiles = new string[] { "monitoring.csv",
  "monitoring_hr.csv",
  "monitoring_rr.csv",
  "daily_summary.csv",
  "resting_hr.csv",
  "sleep.csv",
  "stress.csv",
  "sleep_events.csv"};

var strScript1FileName = "garmindb_cli.py";
var strScript2FileName = "sqlitetocsv.py";
var strScript3FileName = "upload_to_influx.py";
var strInfluxBucket = ""; //Set
var strScript1Parameters = "";
var intDays = -5;
var strInfluxOrg = ""; //Set
var strInfluxDB = "http://192.168.50.115:8086"; //Set
var strToken = ""; //Set

if (File.Exists(strUserPath + "\\HealthData\\DBs\\garmin.db"))
{
    Console.WriteLine("Subsequent Run");
    strScript1Parameters = "--sleep --rhr --monitoring --activities --download --import --analyze --latest";
}
else
{
    Console.WriteLine("First Run");
    strScript1Parameters = "--sleep --rhr --monitoring --activities --download --import --analyze";
    Directory.CreateDirectory(strDesktopPath + "\\" + strCSVOutputFolderName);
}

Console.WriteLine("Run Garmin Script: " + strPythonPath + " \"" + strDesktopPath + "\\" + strScript1FileName + "\" " + strScript1Parameters);
var command = strDesktopPath + "\\" + strScript1FileName + " " + strScript1Parameters;
ProcessStartInfo cmdsi = new ProcessStartInfo(strPythonPath);
cmdsi.Arguments = command;
Process cmd = Process.Start(cmdsi);
cmd.WaitForExit();
cmd.Close();
Parallel.ForEach(arrDBNames, db =>
{
    Console.WriteLine("Run sqlite to csv " + strPythonPath + " \"" + strDesktopPath + "\\" + strScript2FileName + "\" " + "\"" + strUserPath + "\\HealthData\\DBs\\" + db + "\" " + "\"" + strDesktopPath + "\\" + strCSVOutputFolderName + "\" ");
    string command = strDesktopPath + "\\" + strScript2FileName + " " + "\"" + strUserPath + "\\HealthData\\DBs\\" +
             db + "\" " + "\"" + strDesktopPath + "\\" + strCSVOutputFolderName + "\\";
    var cmdsi = new ProcessStartInfo(strPythonPath)
    {
        Arguments = command
    };
    using (var cmd = Process.Start(cmdsi))
    {
        if (cmd != null)
        {
            cmd.WaitForExit();
        }
    }
});

Console.WriteLine("Pull CSVs");
var dtmonitoring = ReadCsv(strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[0]);
Console.WriteLine(dtmonitoring.Rows.Count);
var dtmonitoring_hr = ReadCsv(strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[1]);
Console.WriteLine(dtmonitoring_hr.Rows.Count);
var dtmonitoring_rr = ReadCsv(strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[2]);
Console.WriteLine(dtmonitoring_rr.Rows.Count);
var dtdaily_summary = ReadCsv(strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[3]);
Console.WriteLine(dtdaily_summary.Rows.Count);
var dtresting_hr = ReadCsv(strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[4]);
Console.WriteLine(dtresting_hr.Rows.Count);
var dtsleep = ReadCsv(strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[5]);
Console.WriteLine(dtsleep.Rows.Count);
var dtstress = ReadCsv(strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[6]);
Console.WriteLine(dtstress.Rows.Count);
var dtsleep_events = ReadCsv(strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[7]);
Console.WriteLine(dtsleep_events.Rows.Count);
Console.WriteLine("Filter Files");
var dtoutmonitoring = FilterDataTable(dtmonitoring, intDays);
var dtoutmonitoring_hr = FilterDataTable(dtmonitoring_hr, intDays);
var dtoutmonitoring_rr = FilterDataTable(dtmonitoring_rr, intDays);
var dtoutdaily_summary = FilterRowsByDay(dtdaily_summary, intDays);
var dtoutresting_hr = FilterRowsByDay(dtresting_hr, intDays);
var dtoutsleep = FilterRowsByDay(dtsleep, intDays);
var dtoutstress = FilterDataTable(dtstress, intDays);
var dtoutsleep_events = FilterDataTable(dtsleep_events, intDays);
Console.WriteLine("Write filtered data back to CSV");
WriteDataTableToCSV(dtoutmonitoring, strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[0]);
WriteDataTableToCSV(dtoutmonitoring_hr, strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[1]);
WriteDataTableToCSV(dtoutmonitoring_rr, strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[2]);
WriteDataTableToCSV(dtoutdaily_summary, strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[3]);
WriteDataTableToCSV(dtoutresting_hr, strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[4]);
WriteDataTableToCSV(dtoutsleep, strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[5]);
WriteDataTableToCSV(dtoutstress, strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[6]);
WriteDataTableToCSV(dtoutsleep_events, strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + arrCSVFiles[7]);
var index = 0;
foreach (var db in arrCSVFiles)
{
    Console.WriteLine(strDesktopPath + "\\" + strScript3FileName + " " +
             "--url " + "\"" + strInfluxDB + "\" " +
             "--token " + "\"" + strToken + "\" " +
             "--org " + "\"" + strInfluxOrg + "\" " +
             "--bucket " + "\"" + strInfluxBucket + "\" " +
             "--file " + "\"" + strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + db + "\" " +
             "--measurement " + "\"" + arrCSVNames[index] + "\"");
    command = strDesktopPath + "\\" + strScript3FileName + " " +
         "--url " + "\"" + strInfluxDB + "\" " +
         "--token " + "\"" + strToken + "\" " +
         "--org " + "\"" + strInfluxOrg + "\" " +
         "--bucket " + "\"" + strInfluxBucket + "\" " +
         "--file " + "\"" + strDesktopPath + "\\" + strCSVOutputFolderName + "\\" + db + "\" " +
         "--measurement " + "\"" + arrCSVNames[index] + "\"";
    cmdsi = new ProcessStartInfo(strPythonPath)
    {
        Arguments = command
    };

    using (cmd = Process.Start(cmdsi))
    {
        if (cmd != null)
        {
            cmd.WaitForExit();
        }
    }
    index++;
}