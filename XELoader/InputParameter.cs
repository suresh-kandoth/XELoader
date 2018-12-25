using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.XEvent;
using Microsoft.SqlServer.XEvent.Linq;
using System.Xml;
using System.Security;

namespace XELoader
{
    public class InputParameters
    {
        public int m_Count_Parameters = 0;

        public String m_XEL_File_To_Process = "";                       // populated by -f
        public String m_XEM_File_To_Process = "";                       // populated by -m
        public String m_XE_Directory_To_Process = "";                   // populated by -D
        public String m_XEL_File_Pattern = "*.xel";                     // populated by -p
        public String m_Version_Of_Collected_Logs;                      // populated by -v  [not used right now, keeping it for future use]

        public String m_Destination_SQL_Server = ".";                   // populated by -S  [upper case]
        public String m_Destination_SQL_Database = "XE_Import";         // populated by -d
        public String m_Destination_SQL_Login = "";                     // populated by -U  [upper case]
        public String m_Destination_SQL_Password = "";                  // populated by -P  [upper case]
        public String m_Destination_Security_Mode = "Integrated";
        public SqlCredential m_Destination_Sql_Credential;

        public int m_bulkcopy_BatchSize = 1048576;                      // populated by -b  [setting it for the optimal size needed for columnstore]
        public String m_IndexType = "";                                 // populated by -I  [allowed values are RowStore and ColumnStore]
        public String m_SchemaName = "xel";                             // populated by -s  [lower case]
        public bool m_AppendToExistingData = false;                     // populated by -a
        public bool m_ClearExistingData = false;                        // populated by -c
        public bool m_WipeDatabase = false;                             // populated by -w
        public int m_NumThreads = 0;                                    // populated by -t  [this value will get reset to number of processors on the system upto a max of 16]
        public String m_ReadAhead = "y";                                // populated by -R
        public String m_timeZone = "Local";                             // populated by -z
        public TimeZoneInfo m_timeZoneInfo;                             // constructed after processing -z parameter
        public int m_StringToStringTruncation = 1024;                   // populated by -L
        public int m_XMLToStringTruncation = 4000;                      // populated by -X
        public bool m_storeXMLasString = false;                         // populated by -x
        public int m_BinaryToBinaryTruncation = 1024;                   // populated by -B
        public int m_errorCount = 100;                                  // populated by -e
        public bool m_LOBallowedonCSI = false;                          // populated by checking server version and auto detect
        public bool m_disableLOBonCSI = false;                          // populated by -l

        public int m_CallStack_FrameLength = 8;                         // populated by -h
        public String m_CallStack_command = "ln";                       // populated by -j

        public String m_ConnectionString_targetDB = "";                 // constructed using server name and target database name to which events are loaded
        public String m_ConnectionString_masterDB = "";                 // constructed using server name and master database name

        // this method parses all input parameters and stores the value in the member variables
        public bool ProcessInputParameters(String[] _input_params)
        {
            int param_length;
            foreach (String _input_param in _input_params)
            {
                m_Count_Parameters++;
                param_length = _input_param.Length;
                switch (_input_param.Substring(1, 1))
                {
                    case "?":
                        {
                            m_Count_Parameters--;
                            break;
                        }
                    case "f":
                        {
                            m_XEL_File_To_Process = _input_param.Substring(2);
                            break;
                        }
                    case "m":
                        {
                            m_XEM_File_To_Process = _input_param.Substring(2);
                            break;
                        }
                    case "D":
                        {
                            char[] charsToTrim = { '\\', '"' };    // we need to get rid of extra characters at the end of the directory path
                            String s_XE_Directory_To_Process = _input_param.Substring(2);
                            m_XE_Directory_To_Process = s_XE_Directory_To_Process.TrimEnd(charsToTrim);
                            break;
                        }
                    case "p":
                        {
                            m_XEL_File_Pattern = _input_param.Substring(2);
                            break;
                        }
                    case "v":
                        {
                            m_Version_Of_Collected_Logs = _input_param.Substring(2);
                            break;
                        }
                    case "S":
                        {
                            m_Destination_SQL_Server = _input_param.Substring(2);
                            break;
                        }
                    case "d":
                        {
                            m_Destination_SQL_Database = _input_param.Substring(2);
                            break;
                        }
                    case "b":
                        {
                            m_bulkcopy_BatchSize = Convert.ToInt32(_input_param.Substring(2));
                            break;
                        }
                    case "I":
                        {
                            m_IndexType = _input_param.Substring(2);
                            break;
                        }
                    case "s":
                        {
                            m_SchemaName = _input_param.Substring(2);
                            break;
                        }
                    case "w":
                        {
                            m_WipeDatabase = true;
                            break;
                        }
                    case "a":
                        {
                            m_AppendToExistingData = true;
                            break;
                        }
                    case "c":
                        {
                            m_ClearExistingData = true;
                            break;
                        }
                    case "t":
                        {
                            m_NumThreads = Convert.ToInt32(_input_param.Substring(2));
                            break;
                        }
                    case "R":
                        {
                            m_ReadAhead = _input_param.Substring(2);
                            break;
                        }
                    case "z":
                        {
                            m_timeZone = _input_param.Substring(2);
                            break;
                        }
                    case "L":
                        {
                            m_StringToStringTruncation = Convert.ToInt32(_input_param.Substring(2));
                            break;
                        }
                    case "X":
                        {
                            m_XMLToStringTruncation = Convert.ToInt32(_input_param.Substring(2));
                            break;
                        }
                    case "x":
                        {
                            m_storeXMLasString = true;
                            break;
                        }
                    case "l":
                        {
                            m_disableLOBonCSI = true;
                            break;
                        }
                    case "B":
                        {
                            m_BinaryToBinaryTruncation = Convert.ToInt32(_input_param.Substring(2));
                            break;
                        }
                    case "e":
                        {
                            m_errorCount = Convert.ToInt32(_input_param.Substring(2));
                            break;
                        }
                    case "h":
                        {
                            m_CallStack_FrameLength = Convert.ToInt32(_input_param.Substring(2));
                            break;
                        }
                    case "j":
                        {
                            m_CallStack_command = _input_param.Substring(2);
                            break;
                        }
                    case "U":   // SQL Server login name
                        {
                            m_Destination_SQL_Login = _input_param.Substring(2);
                            m_Destination_Security_Mode = "Standard";
                            break;
                        }
                    case "P":   // SQL Server password
                        {
                            m_Destination_SQL_Password = _input_param.Substring(2);
                            SecureString securePwd = new SecureString();
                            char[] charArr = m_Destination_SQL_Password.ToCharArray();
                            for (int i=0; i<m_Destination_SQL_Password.Length; i++)
                            {
                                securePwd.AppendChar(charArr[i]);
                            }
                            securePwd.MakeReadOnly();
                            m_Destination_Sql_Credential = new SqlCredential(m_Destination_SQL_Login, securePwd);
                            m_Destination_SQL_Password = "";
                            break;
                        }
                    default:
                        {
                            break;
                        }
                }   //switch
            }   // foreach


            if (1 > m_Count_Parameters) // there is a problem with the parameters passed
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(" ");
                Console.WriteLine("*** There is a problem with the parameters supplied ***");
                Console.WriteLine("    There are two required parameters: file location and SQL Server name");
                Console.WriteLine("    Double check to make sure there are no escape characters or special characters in the parameters");
                Console.WriteLine("    Do not specify a trailing backslash for the path in the parameters");
                Console.ResetColor();
                PrintParameterHelp();
                return false;
            }
            if (1 <= m_Count_Parameters) // now we have a good set of parameters to process
            {
                // prepare connection string for easy use later
                if ("Integrated" == m_Destination_Security_Mode)
                {
                    m_ConnectionString_targetDB = @"Server=" + m_Destination_SQL_Server + @"; database=" + m_Destination_SQL_Database + @"; Trusted_Connection=yes;Timeout=60;";
                    m_ConnectionString_masterDB = @"Server=" + m_Destination_SQL_Server + @"; database=master; Trusted_Connection=yes;Timeout=60;";
                }
                if ("Standard" == m_Destination_Security_Mode)
                {
                    m_ConnectionString_targetDB = @"Server=" + m_Destination_SQL_Server + @"; database=" + m_Destination_SQL_Database + @"; Timeout=60;";
                    m_ConnectionString_masterDB = @"Server=" + m_Destination_SQL_Server + @"; database=master; Timeout=60;";
                }

                if (0 == m_NumThreads)
                {
                    // if the user did not provide the number of threads to use, then create one thread for every processor on the system
                    // limit it to 8 processors since we do not want to overload the disk systems

                    m_NumThreads = Math.Min(System.Environment.ProcessorCount, 8);
                }

                // print time zone information
                try
                {
                    TimeZoneInfo localZone = (m_timeZone == "Local") ? TimeZoneInfo.Local : TimeZoneInfo.FindSystemTimeZoneById(m_timeZone);
                    Console.WriteLine("Local Time Zone ID in use : {0} [{1}]", localZone.Id, localZone.DaylightName);
                    Console.WriteLine("   To override this UTC to LOCAL time conversion pass in the timezone ID of interest to -z parameter");
                    Console.WriteLine(@"   Valid timezone values @ HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Windows NT\CurrentVersion\Time Zones");
                    Console.WriteLine("");
                    m_timeZoneInfo = localZone;
                }
                catch (TimeZoneNotFoundException)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Unable to find the {0} zone in this system.", m_timeZone);
                    Console.WriteLine("Use a valid timezone from the following list:");
                    ReadOnlyCollection<TimeZoneInfo> tzCollection;
                    tzCollection = TimeZoneInfo.GetSystemTimeZones();
                    foreach (TimeZoneInfo timeZone in tzCollection)
                        Console.WriteLine("   ID [{0}]     :: [{1}]", timeZone.Id, timeZone.DisplayName);
                    Console.WriteLine("");
                    Console.ResetColor();
                    throw;
                }

                return true;
            }

            return false;
        }   //ProcessInputParameters

        public void DetectServerCapabilities()
        {
            // Establish a connection to the SQL Server that we need to query
            SqlConnection DestinationConnection = new SqlConnection(m_ConnectionString_masterDB);
            if ("Standard" == m_Destination_Security_Mode)
            {
                DestinationConnection.Credential = m_Destination_Sql_Credential;
            }
            DestinationConnection.Open();

            // check the server version
            String ProductVersion = "";
            String tsql_VersionCheck = "SELECT SERVERPROPERTY('ProductVersion')";
            SqlCommand sqlcmd_VersionCheck = new SqlCommand(tsql_VersionCheck, DestinationConnection);
            try
            {
                ProductVersion = (String)sqlcmd_VersionCheck.ExecuteScalar();
            }
            catch (SqlException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : Error while querying server version", Thread.CurrentThread.ManagedThreadId);
                Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                Console.ResetColor();
            }
            int MajorVersion = Convert.ToInt32(ProductVersion.Substring(0, 2));
            // now depending on the major version we can enable or disable certain features to use
            if ((12 > MajorVersion) && ("" == m_IndexType))
            {
                // for server versions below 12 [sql 2014] there is no updateable columnstore index support, so we will just use RowStore
                m_IndexType = "RowStore";
            }
            if ((12 <= MajorVersion) && ("RowStore" != m_IndexType))
            {
                // for server versions above 12 [sql 2014] we can use updateable columnstore index support, so we use ColumnStore if RowStore not requested explicitly
                m_IndexType = "ColumnStore";
            }
            if ((14 <= MajorVersion) && ("ColumnStore" == m_IndexType) && (false == m_disableLOBonCSI))
            {
                // for server versions above 14 [sql 2017] we can use lob columns in updateable columnstore index support
                m_LOBallowedonCSI = true;
            }
            Console.WriteLine("Thread {0} : Using target SQL Server version {1} ", Thread.CurrentThread.ManagedThreadId, ProductVersion);
            Console.WriteLine("Thread {0} : Using index type : {1} [Use -I parameter to override]", Thread.CurrentThread.ManagedThreadId, m_IndexType);
            if ("ColumnStore" == m_IndexType)
            {
                Console.WriteLine("Thread {0} : Using large data types : {1} [Use -l parameter to override]", Thread.CurrentThread.ManagedThreadId, m_LOBallowedonCSI.ToString());
                if (false == m_LOBallowedonCSI)
                    Console.WriteLine("Thread {0} : Max data lengths : nvarchar({1}) , varbinary({2}) [Use parameters -X / -L / -B to override]", Thread.CurrentThread.ManagedThreadId, m_StringToStringTruncation, m_BinaryToBinaryTruncation);
            }
        }

        public void CreateDatabase()
        {
            // Establish a connection to the SQL Server where the database needs to be created
            SqlConnection DestinationConnection = new SqlConnection(m_ConnectionString_masterDB);
            if ("Standard" == m_Destination_Security_Mode)
            {
                DestinationConnection.Credential = m_Destination_Sql_Credential;
            }
            DestinationConnection.Open();

            // check if the database requested exists in this server
            int RowCount = 0;
            String tsql_UseDatabase = "select count([name]) from sys.databases where [name] = N'" + m_Destination_SQL_Database + "'";
            SqlCommand sqlcmd_DatabaseCheck = new SqlCommand(tsql_UseDatabase, DestinationConnection);
            try
            {
                RowCount = (int)sqlcmd_DatabaseCheck.ExecuteScalar();
            }
            catch (SqlException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : Error while querying metadata", Thread.CurrentThread.ManagedThreadId);
                Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                Console.ResetColor();
            }

            // if the database exists check if we can reuse or wipe and then recreate
            if (1 == RowCount)
            {
                if (true == m_WipeDatabase)
                {
                    String tsql_SingleUser = "ALTER DATABASE [" + m_Destination_SQL_Database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE";
                    SqlCommand sqlcmd_SingleUser = new SqlCommand(tsql_SingleUser, DestinationConnection);
                    String tsql_DropDatabase = "DROP DATABASE [" + m_Destination_SQL_Database + "]";
                    SqlCommand sqlcmd_DropDatabase = new SqlCommand(tsql_DropDatabase, DestinationConnection);
                    try
                    {
                        Console.WriteLine("Thread {0} : Dropping database {1} as requested by parameter combinations [Use -w, -c, -a to change behavior]", Thread.CurrentThread.ManagedThreadId, m_Destination_SQL_Database);
                        sqlcmd_SingleUser.ExecuteNonQuery();
                        sqlcmd_DropDatabase.ExecuteNonQuery();
                    }
                    catch (SqlException ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Thread {0} : Error while dropping database", Thread.CurrentThread.ManagedThreadId);
                        Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                        Console.ResetColor();
                    }
                    // set rowcount to zero so that we will create the database next
                    RowCount = 0;
                }
                else
                {
                    Console.WriteLine("Thread {0} : database {1} already exists, re-using it to store data [use -w to wipe out entire database and recreate it]", Thread.CurrentThread.ManagedThreadId, m_Destination_SQL_Database);
                }
            }

            // we need to create the requested database if it does not exist already
            if (0 == RowCount)
            {
                // Establish the create database command
                String tsql_CreateDatabase = "CREATE DATABASE [" + m_Destination_SQL_Database + "]";
                SqlCommand sqlcmd_CreateDatabase = new SqlCommand(tsql_CreateDatabase, DestinationConnection);
                String tsql_RecoveryModel = "ALTER DATABASE [" + m_Destination_SQL_Database + "] SET RECOVERY BULK_LOGGED";
                SqlCommand sqlcmd_RecoveryModel = new SqlCommand(tsql_RecoveryModel, DestinationConnection);
                String tsql_BackupDatabase = "BACKUP DATABASE [" + m_Destination_SQL_Database + "] TO DISK = 'NUL'";
                SqlCommand sqlcmd_BackupDatabase = new SqlCommand(tsql_BackupDatabase, DestinationConnection);
                try
                {
                    Console.WriteLine("Thread {0} : Creating database {1} as requested by parameter combinations [use -w, -c, -a to change behavior]", Thread.CurrentThread.ManagedThreadId, m_Destination_SQL_Database);
                    sqlcmd_CreateDatabase.ExecuteNonQuery();
                    sqlcmd_RecoveryModel.ExecuteNonQuery();
                    sqlcmd_BackupDatabase.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Thread {0} : Error while create database and setting options", Thread.CurrentThread.ManagedThreadId);
                    Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                    Console.ResetColor();
                }
            }

            // cleanup resources
            DestinationConnection.Close();
        }

        public void CreateSchema()
        {
            SqlConnection DestinationConnection = new SqlConnection(m_ConnectionString_targetDB);
            if ("Standard" == m_Destination_Security_Mode)
            {
                DestinationConnection.Credential = m_Destination_Sql_Credential;
            }
            DestinationConnection.Open();

            // Create the schema supplied as input parameter
            if ("dbo" != m_SchemaName)
            {
                // first do a check to see if the schema requested exists
                int RowCount = 0;
                String tsql_SchemaCheck = "select count([name]) from sys.schemas where [name] = N'" + m_SchemaName + "'";
                SqlCommand sqlcmd_SchemaCheck = new SqlCommand(tsql_SchemaCheck, DestinationConnection);
                try
                {
                    RowCount = (int)sqlcmd_SchemaCheck.ExecuteScalar();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                }

                // the schema requested does not exist, so go ahead and create the schema requested
                if (RowCount < 1)
                {
                    String tsql_CreateSchema = "CREATE SCHEMA [" + m_SchemaName + "]";
                    SqlCommand sqlcmd_CreateSchema = new SqlCommand(tsql_CreateSchema, DestinationConnection);
                    try
                    {
                        sqlcmd_CreateSchema.ExecuteNonQuery();
                    }
                    catch (SqlException ex)
                    {
                        Console.WriteLine("Thread {0} : Msg : {1} {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                    }
                }
            }
            // cleanup resources
            DestinationConnection.Close();
        }

        public void CreateTrackingTable()
        {
            // Establish a connection to the SQL Server where the table needs to be created
            SqlConnection DestinationConnection = new SqlConnection(m_ConnectionString_targetDB);
            if ("Standard" == m_Destination_Security_Mode)
            {
                DestinationConnection.Credential = m_Destination_Sql_Credential;
            }
            DestinationConnection.Open();

            // check if this table already exists
            int RowCount = 0;
            String tsql_ObjectCheck = "select count([name]) from sys.tables where [name] = N'tbl_ImportedXEventFiles' and schema_id = SCHEMA_ID('dbo')";
            SqlCommand sql_cmd_ObjectCheck = new SqlCommand(tsql_ObjectCheck, DestinationConnection);
            try
            {
                RowCount = (int)sql_cmd_ObjectCheck.ExecuteScalar();
            }
            catch (SqlException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                Console.ResetColor();
            }

            // create the table if it does not already exist
            if (RowCount < 1)
            {
                String tsql_CreateTable =
                    "CREATE TABLE [dbo].[tbl_ImportedXEventFiles] ([file_id] bigint identity(1,1) primary key clustered, [file_folder] nvarchar(2000) null, [file_name] nvarchar(2000) null, [total_processing_time] time null, [bulk_copy_time] time null)";
                SqlCommand sql_cmd_create = new SqlCommand(tsql_CreateTable, DestinationConnection);
                // Execute the create table command
                try
                {
                    sql_cmd_create.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                    Console.ResetColor();
                }
            }
            // cleanup resources
            DestinationConnection.Close();

        }

        public void PrintParameterHelp()
        {
            Console.WriteLine("");
            Console.WriteLine("*** Parameter Details ***");
            Console.WriteLine("*** Parameters are case sensitive, no spaces allowed after parameter name and its value ***");
            Console.WriteLine("");
            Console.WriteLine("*** Required Parameters ***");
            Console.WriteLine("    Use -D or -f for input files to feed");
            Console.WriteLine("    Utility expects file names to be in thier original format: SessionName_PartitionID_TimeStampInfo.xel");
            Console.WriteLine("");
            Console.WriteLine("-D : specify path to a folder of .xel files that needs to be processed");
            Console.WriteLine("      do not specify the trailing slash for the folder, keep the files local to the machine for fast performance");
            Console.WriteLine("      if this folder contains .xel files from multiple XE session, use -p parameter to specify file pattern to process");
            Console.WriteLine("-f : specify path to a single .xel file that needs to be processed");
            Console.WriteLine("      if there are multiple files from the same XE session then use the -D parameter");
            Console.WriteLine("-S : specify the server name and instance name to which you want to save the XE data [default : (local) ]");
            Console.WriteLine("      for fast performance use TCP by specifying the server name in the format : -SMachineName,TcpPortNumber");
            Console.WriteLine("");
            Console.WriteLine("*** Optional parameters ***");
            Console.WriteLine("");
            Console.WriteLine("-d : specify the database name to use for saving the processed XE data [default : XE_Import]");
            Console.WriteLine("      if the database name exists, it will be used instead of creating a new one");
            Console.WriteLine("      you can pre-create the database on a specific location, multiple files and other filegroup options for better performance");
            Console.WriteLine("-U : specify the SQL login name to use for connecting to the SQL Server");
            Console.WriteLine("      If you do not specify this parameter, Trusted security or Intergrated authentication is used");
            Console.WriteLine("      If you specify this parameter, supply the password for this login using the -P parameter");
            Console.WriteLine("-P : specify the password for the SQL login name");
            Console.WriteLine("      Use this option if you used the -U parameter");
            Console.WriteLine("-w : use this parameter to indicate if the existing database needs to be (wiped) - dropped and recreated from scratch");
            Console.WriteLine("      you do not specify any value for parameter");
            Console.WriteLine("-c : use this parameter to indicate if all existing tables for this collection of events needs to be cleared and data reloaded");
            Console.WriteLine("      if this option is not passed and the table already exists then new data will be appended to existing tables");
            Console.WriteLine("      you do not specify any value for parameter");
            Console.WriteLine("-a : use this parameter to indicate if data from events needs to be appended to existing tables in the database provided");
            Console.WriteLine("      use this option for incremental loads into an existing database that already has imported information");
            Console.WriteLine("      when this option is specified no metadata work happens [database level and table level]");
            Console.WriteLine("      you do not specify any value for parameter");
            Console.WriteLine("-s : specify the schema under which to create the objects [default : xel]");
            Console.WriteLine("      for each event captured in the XE session a table will be created under this schema");
            Console.WriteLine("-I : specify the Index type used to store the data in the database - RowStore or ColumnStore [default : ColumnStore]");
            Console.WriteLine("      ColumnStore will provide great compression for repeat data and performance while doing aggregate queries");
            Console.WriteLine("      ColumnStore does not support large data types so we have to truncate long strings and xml columns - see parameters -L and -X");
            Console.WriteLine("      RowStore will save all the data in the XE files - so this can use up lot of storage and also slow while querying");
            Console.WriteLine("-b : specify the bulk copy batch size in multiples of 100K [default : 1048576]");
            Console.WriteLine("      when using ColumnStore leave the batch size to the default of 1 million since it is the ideal rowgroup size");
            Console.WriteLine("-t : specify the number of threads to use for processing [default : # of processors in the system capped at 8]");
            Console.WriteLine("      each XEL file will be processed independently on a seperate thread, the more parallelism the faster you can load the files");
            Console.WriteLine("      If you need more than the default 8 threads to process the XEL files in parallel, then provide a value higher than 8");
            Console.WriteLine("      If you need to limit the number of threads processing the XEL files, then provide a value like 1 or 2 for this parameter");
            Console.WriteLine("-p : specify the name pattern of files that needs to be processed from a folder of .xel files [default : \"*.xel\"]");
            Console.WriteLine("      if there are XE files from multiple XE sessions use this parameter to selectively process files of interest");
            Console.WriteLine("      Example to use: -p\"activity_tracing*.xel\" will process all XEL files that start with activity_tracing in that folder");
            Console.WriteLine("-R : specify whether to perform read-ahead (y) or not (n) [default : y]");
            Console.WriteLine("      read ahead can populate the file system cache and potentially improve event processing performance depending on system memory");
            Console.WriteLine("-z : specify what timezone you want to use when processing time information to convert utc time to local time");
            Console.WriteLine("     every event table will have three time columns representing the time of the event");
            Console.WriteLine("      1.UTC time in smalldatetime format used for graphing and other visuals");
            Console.WriteLine("      2.UTC time in datetime2 format used for accurate event sequence down to the 100 nanoseconds unit");
            Console.WriteLine("      3.LOCAL time in datetime2 format used for accurate event sequence down to the 100 nanoseconds unit");
            Console.WriteLine("     If you do not pass any value for this parameter, the local timezone of the importing system is used");
            Console.WriteLine("-l : use this parameter to indicate if you want to disable use of LOB with ColumnStore index in newer versions");
            Console.WriteLine("      SQL Server 2017 and above allow LOB columns on ColumnStore index");
            Console.WriteLine("      you do not specify any value for parameter");
            Console.WriteLine("-L : specify truncation length for large strings when using ColumnStore index [default : 1024]");
            Console.WriteLine("-X : specify truncation length for large XML values when using ColumnStore index [default : 4000]");
            Console.WriteLine("-x : use this parameter to indicate if you store xml values in varchar(max) columns instead of xml columns");
            Console.WriteLine("      preferred for xml values with more than 128 depth level");
            Console.WriteLine("      you do not specify any value for parameter");
            Console.WriteLine("-B : specify truncation length for large binary values when using ColumnStore index [default : 1024]");
            Console.WriteLine("-e : specify number of errors or exceptions you are willing to tolerate [default : 100]");
            Console.WriteLine("      remember that skipping errors while loading the events can affect the accuracy of the data imported");
            Console.WriteLine("-h : specify how many bytes represent one frame in callstack action [default : 8]");
            Console.WriteLine("      if you collected the extended events on a 64-bit server then you need 8 bytes for a frame");
            Console.WriteLine("      if you collected the extended events on a 32-bit server then you need 4 bytes for a frame");
            Console.WriteLine("-j : specify what debugger command you want to execute for each frame in callstack action [default : ln]");
            Console.WriteLine("");
            Console.WriteLine("*** End of parameters ***");
        }
    }
}
