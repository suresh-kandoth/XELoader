using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using Microsoft.SqlServer.XEvent;
using Microsoft.SqlServer.XEvent.Linq;
using System.Xml;

namespace XELoader
{
    public class EventMetadata
    {
        public DataSet m_data_set;
        public DataTableCollection m_event_tables;
        public bool m_dt_Initialized = false;
        public String m_MaxPossibleStringColumn = "";
        public String m_MaxPossibleXMLColumn = "";
        public String m_MaxPossibleBinaryColumn = "";

        public EventMetadata()
        {
            m_data_set = new DataSet();
            m_event_tables = m_data_set.Tables;
        }

        public void ExtractMetadataFromFile(QueryableXEventData in_event_file)
        {
            Console.WriteLine("Thread {0} : Evaluating the metadata of the extended event file and building data tables", Thread.CurrentThread.ManagedThreadId);
            // initialize the max lengths of string, xml and binary columns
            GetMaxPossibleColumn();
            // read the metadata from the QueryableXEventData
            foreach (IMetadataGeneration mgen in in_event_file.EventProvider.MetadataGenerations)
            {
                // collection to store all actions from each package
                DataTable dt_Actions = new DataTable("Actions");
                DataColumnCollection dc_col_actions = dt_Actions.Columns;
                //iterate through each package to extract actions
                foreach (IPackage xe_package in mgen.Packages)
                {
                    //iterate through the actions
                    foreach (IActionMetadata xe_action in xe_package.Actions)
                    {
                        switch (xe_action.Name)
                        {
                            case "debug_break":
                            case "create_dump_single_thread":
                            case "create_dump_all_threads":
                                {   // some actions do not have any valid information in the extended event data file to be processed, so ignore them
                                    break;
                                }
                            case "query_hash":
                            case "query_plan_hash":
                                {   // actions that capture hash values for query execution have some data type mismatch issues, so we create another parallel column with binary data
                                    dc_col_actions.Add("a_" + xe_action.Name, GetDataTableColumnType(xe_action.Type.ToString(), xe_action.Name));
                                    dc_col_actions.Add("a_" + xe_action.Name + "_bin", System.Type.GetType("System.Byte[]"));
                                    break;
                                }
                            case "callstack":
                                {   // actions that capture hash values for query execution have some data type mismatch issues, so we create another parallel column with binary data
                                    dc_col_actions.Add("a_" + xe_action.Name, GetDataTableColumnType(xe_action.Type.ToString(), xe_action.Name));
                                    dc_col_actions.Add("a_" + xe_action.Name + "_debugcmd", System.Type.GetType("System.String"));
                                    break;
                                }
                            default:
                                {   // each action present in this package needs to be added as a column, All actions will have a_ prefix for the column names
                                    dc_col_actions.Add("a_" + xe_action.Name, GetDataTableColumnType(xe_action.Type.ToString(), xe_action.Name));
                                    break;
                                }
                        }
                    }
                }
                // iterate through each package to extract events
                foreach (IPackage xe_package in mgen.Packages)
                {
                    //iterate through each event
                    foreach (IEventMetadata xe_event in xe_package.Events)
                    {
                        //create one datatable per event
                        DataTable dt_event = new DataTable(xe_event.Name);
                        DataColumnCollection dc_col_fields = dt_event.Columns;

                        // Add a mandatory column to indicate which file this event is coming from
                        DataColumn file_id_column;
                        file_id_column = new DataColumn("e_Imported_File_Id", System.Type.GetType("System.Int64"));
                        dt_event.Columns.Add(file_id_column);

                        // Add a mandatory column to capture the time at which event occured: Time_Of_Event
                        DataColumn time_column;
                        time_column = new DataColumn("e_Time_Of_Event", System.Type.GetType("System.DateTime"));
                        dt_event.Columns.Add(time_column);

                        // Add a mandatory column to capture the time at which event occured: Time_Of_Event_utc
                        DataColumn time_column_utc;
                        time_column_utc = new DataColumn("e_Time_Of_Event_utc", System.Type.GetType("System.DateTime"));
                        dt_event.Columns.Add(time_column_utc);

                        // Add a mandatory column to capture the time at which event occured: Time_Of_Event_local
                        DataColumn time_column_local;
                        time_column_local = new DataColumn("e_Time_Of_Event_local", System.Type.GetType("System.DateTime"));
                        dt_event.Columns.Add(time_column_local);

                        //iterate through the fields
                        foreach (IEventFieldMetadata xe_field in xe_event.Fields)
                        {
                            //each field for this event needs to be added as a column, All data columns in a event will have c_ prefix added
                            dc_col_fields.Add("c_" + xe_field.Name, GetDataTableColumnType(xe_field.Type.ToString(), xe_field.Name));
                        }
                        //now add all the actions to the event datatable
                        foreach (DataColumn dc_Action in dc_col_actions)
                        {
                            dt_event.Columns.Add(dc_Action.ColumnName, dc_Action.DataType);
                        }
                        //now add this data table to the global collection
                        m_event_tables.Add(dt_event);
                        //increment table count in the status tracker
                        XELoader.FileProcessor.myTrackStatus.m_Number_of_Tables++;
                        // create the table in the target SQL database for this event
                        // we will do this only if the mode is not append only
                        if (false == XELoader.FileProcessor.myTrackStatus.IsTableCreated)
                        {
                            CreateTableInSQLdatabase(dt_event);
                        }
                    }
                }
            }
            Console.WriteLine("Thread {0} : Finished metadata work, now ready to start processing and load data", Thread.CurrentThread.ManagedThreadId);
        }

        public void CreateTableInSQLdatabase(DataTable in_dt_event)
        {
            // Establish a connection to the SQL Server where the table needs to be created
            SqlConnection DestinationConnection = new SqlConnection(XELoader.FileProcessor.myInputParameters.m_ConnectionString);
            DestinationConnection.Open();

            // check if this table already exists
            int RowCount = 0;
            String tsql_ObjectCheck = "select count([name]) from sys.tables where [name] = N'" + in_dt_event.TableName + "' and schema_id = SCHEMA_ID('" + XELoader.FileProcessor.myInputParameters.m_SchemaName + "')";
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

            // if the table exists and -c requested cleanup then we need to drop the table
            if (RowCount > 0 & true == XELoader.FileProcessor.myInputParameters.m_ClearExistingData)
            {
                String tsql_DropTable = "DROP TABLE [" + XELoader.FileProcessor.myInputParameters.m_SchemaName + "].[" + in_dt_event.TableName + "]";
                SqlCommand sql_cmd_drop = new SqlCommand(tsql_DropTable, DestinationConnection);
                try
                {
                    sql_cmd_drop.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                    Console.ResetColor();
                }
                RowCount = 0;
            }

            // if the table does not exist, then create it
            if (RowCount < 1)
            {
                Console.WriteLine("Thread {0} : creating table to store event : {1} ", Thread.CurrentThread.ManagedThreadId, in_dt_event.TableName);
                // Establish the create table command based on the structure and schema of the data table
                String tsql_CreateTable = "CREATE TABLE [" + XELoader.FileProcessor.myInputParameters.m_SchemaName + "].[" + in_dt_event.TableName + "] (\n";
                // add all columns
                foreach (DataColumn column in in_dt_event.Columns)
                {
                    tsql_CreateTable += "[" + column.ColumnName + "]    " + GetSQLType(column.DataType.FullName, column.ColumnName) + " NULL ,\n";
                }
                // add the end of statement
                tsql_CreateTable = tsql_CreateTable.TrimEnd(new char[] { ',', '\n' }) + "\n)";
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

                // Build the appropriate indexes
                // For rowstore create clustered index on EventSequence
                if ("RowStore" == XELoader.FileProcessor.myInputParameters.m_IndexType)
                {
                    String tsql_ClusteredIndex = "CREATE CLUSTERED INDEX [" + XELoader.FileProcessor.myInputParameters.m_SchemaName + "_" + in_dt_event.TableName + "_ci] ON [" + XELoader.FileProcessor.myInputParameters.m_SchemaName + "].[" + in_dt_event.TableName + "](c_event_sequence) WITH ( DATA_COMPRESSION = PAGE )";
                    SqlCommand sql_cmd_clustered_index = new SqlCommand(tsql_ClusteredIndex, DestinationConnection);
                    try
                    {
                        sql_cmd_clustered_index.ExecuteNonQuery();
                    }
                    catch (SqlException ex)
                    {
                        if (1911 == ex.Number)  // The column specified in the index definition does not exist
                        {
                            String tsql_ClusteredIndex_alt = "CREATE CLUSTERED INDEX [" + XELoader.FileProcessor.myInputParameters.m_SchemaName + "_" + in_dt_event.TableName + "_ci] ON [" + XELoader.FileProcessor.myInputParameters.m_SchemaName + "].[" + in_dt_event.TableName + "](e_Time_Of_Event) WITH ( DATA_COMPRESSION = PAGE )";
                            SqlCommand sql_cmd_clustered_index_alt = new SqlCommand(tsql_ClusteredIndex_alt, DestinationConnection);
                            sql_cmd_clustered_index_alt.ExecuteNonQuery();
                        }
                        else
                        {
                            Console.ForegroundColor = ConsoleColor.Red;
                            Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                            Console.ResetColor();
                        }
                    }
                }
                // For columnstore create clustered columnstore index
                if ("ColumnStore" == XELoader.FileProcessor.myInputParameters.m_IndexType)
                {
                    String tsql_ClusteredIndex = "CREATE CLUSTERED COLUMNSTORE INDEX [" + XELoader.FileProcessor.myInputParameters.m_SchemaName + "_" + in_dt_event.TableName + "_ci] ON [" + XELoader.FileProcessor.myInputParameters.m_SchemaName + "].[" + in_dt_event.TableName + "]";
                    SqlCommand sql_cmd_clustered_index = new SqlCommand(tsql_ClusteredIndex, DestinationConnection);
                    try
                    {
                        sql_cmd_clustered_index.ExecuteNonQuery();
                    }
                    catch (SqlException ex)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                        Console.ResetColor();
                    }
                }
            }
            else
            {
                Console.WriteLine("Thread {0} : Events will append to existing table : {1}  [Overide with -c for clearing out existing content]", Thread.CurrentThread.ManagedThreadId, in_dt_event.TableName);
            }

            // Close the connection
            DestinationConnection.Close();
        }

        public System.Type GetDataTableColumnType(String in_data_type, String in_ColumnName)
        {
            switch (in_data_type)
            {
                case "System.UInt64":
                    return System.Type.GetType("System.Decimal");
                case "System.UInt32":
                case "System.UInt16":
                case "System.UInt8":
                case "System.Int64":
                case "System.Int32":
                case "System.Int16":
                case "System.Int8":
                case "System.DateTime":
                case "System.Byte[]":
                case "System.Boolean":
                    return System.Type.GetType(in_data_type);
                case "Microsoft.SqlServer.XEvent.MapValue":
                case "Microsoft.SqlServer.XEvent.ActivityId":
                case "Microsoft.SqlServer.XEvent.XMLData":
                case "System.String":
                    return System.Type.GetType("System.String");
                default:
                    return System.Type.GetType("System.String");
            }
        }

        public String GetSQLType(String in_data_type, String in_ColumnName)
        {
            switch (in_data_type)
            {
                case "System.Decimal":
                case "System.UInt64":
                    return "decimal(38,0)";
                case "System.Int64":
                case "System.UInt32":
                    return "bigint";
                case "System.Int32":
                case "System.UInt16":
                    return "int";
                case "System.UInt8":
                case "System.Int16":
                    return "smallint";
                case "System.Int8":
                    return "tinyint";
                case "System.Boolean":
                    return "bit";
                case "System.DateTime":
                    switch (in_ColumnName)
                    {
                        case "e_Time_Of_Event":
                            return "smalldatetime";
                        default:
                            return "datetime2(7)";
                    }
                case "System.Byte[]":
                    switch (in_ColumnName)
                    {
                        case "a_callstack":
                        case "c_data_stream":             // we need to figure out later if we need to import data_stream from rpc events since we cannot read them without a decoder
                        default:
                            return m_MaxPossibleBinaryColumn;
                    }
                case "System.String":
                    {
                        switch (in_ColumnName)
                        {
                            // known large data types that need special handling
                            case "c_message":
                            case "c_statement":
                            case "c_batch_text":
                            case "c_execution_statistics":
                            case "a_sql_text":
                                return m_MaxPossibleStringColumn;
                            // known xml data columns and actions
                            case "a_tsql_stack":
                            case "a_tsql_frame":
                            case "c_blocked_process":
                            case "c_calculator":
                            case "c_data":
                            case "c_execution_stats_report":
                            case "c_input_relation":
                            case "c_output_parameters":
                            case "c_server_memory_grants":
                            case "c_showplan_xml":
                            case "c_stats_collection":
                            case "c_xml_report":
                                return m_MaxPossibleXMLColumn;
                            default:
                                return m_MaxPossibleStringColumn;
                        }
                    }
                default:
                    return m_MaxPossibleStringColumn;

            }
        }

        public String GetMaxPossibleStringColumn()
        {
            String _tlength = XELoader.FileProcessor.myInputParameters.m_StringToStringTruncation.ToString();
            // Columnstore does not support max data types in SQL 2014 and 2016
            if (("ColumnStore" == XELoader.FileProcessor.myInputParameters.m_IndexType) && (false == XELoader.FileProcessor.myInputParameters.m_LOBallowedonCSI))
                return String.Concat("nvarchar(", _tlength, ")");
            // for rowstore we will store as max data type
            else
                return "nvarchar(max)";
        }

        public String GetMaxPossibleXMLColumn()
        {
            String _tlength = XELoader.FileProcessor.myInputParameters.m_XMLToStringTruncation.ToString();
            // Columnstore does not support max data types in SQL 2014 and 2016
            if (("ColumnStore" == XELoader.FileProcessor.myInputParameters.m_IndexType) && (false == XELoader.FileProcessor.myInputParameters.m_LOBallowedonCSI))
                return String.Concat("nvarchar(", _tlength, ")");
            // store big xml documents as nvarchar - otherwise you will encounter more than 128 nested depth error
            else if ((("RowStore" == XELoader.FileProcessor.myInputParameters.m_IndexType) && (true == XELoader.FileProcessor.myInputParameters.m_storeXMLasString))
                || (("ColumnStore" == XELoader.FileProcessor.myInputParameters.m_IndexType) && (true == XELoader.FileProcessor.myInputParameters.m_LOBallowedonCSI)))
                return "nvarchar(max)";
            // for rowstore we will store as native XML
            else
                return "xml";
        }

        public String GetMaxPossibleBinaryColumn()
        {
            String _tlength = XELoader.FileProcessor.myInputParameters.m_BinaryToBinaryTruncation.ToString();
            // Columnstore does not support max data types in SQL 2014 and 2016
            if (("ColumnStore" == XELoader.FileProcessor.myInputParameters.m_IndexType) && (false == XELoader.FileProcessor.myInputParameters.m_LOBallowedonCSI))
                return String.Concat("varbinary(", _tlength, ")");
            // for rowstore we will store as max data type
            else
                return "varbinary(max)";
        }

        public void GetMaxPossibleColumn()
        {
            m_MaxPossibleStringColumn = GetMaxPossibleStringColumn();
            m_MaxPossibleXMLColumn = GetMaxPossibleXMLColumn();
            m_MaxPossibleBinaryColumn = GetMaxPossibleBinaryColumn();
        }
    }

}
