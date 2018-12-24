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
    public class EventHolder
    {
        private DataSet m_data_set;
        private DataTableCollection m_event_tables;
        public TimeSpan m_Total_Processing_Duration;
        public TimeSpan m_Bulk_Copy_Duration;
        public Int64 m_fileID;
        public FileInfo m_event_File_Info;
        public int m_Number_of_Errors;
        public int m_Number_of_String_Truncations;
        public int m_Number_of_XML_Truncations;
        public int m_Number_of_Binary_Truncations;

        public EventHolder(String in_FileName)
        {
            m_event_File_Info = new FileInfo(in_FileName);
            m_data_set = XELoader.FileProcessor.myEventMetadata.m_data_set.Clone();
            m_event_tables = m_data_set.Tables;
            m_Bulk_Copy_Duration = new TimeSpan(0, 0, 0, 0, 0);
            m_Total_Processing_Duration = new TimeSpan(0, 0, 0, 0, 0);
            m_fileID = 0;
            m_Number_of_Errors = 0;
            m_Number_of_String_Truncations = 0;
            m_Number_of_XML_Truncations = 0;
            m_Number_of_Binary_Truncations = 0;
        }

        public void ProcessEvent(PublishedEvent in_x_event)
        {
            bool retVal = false;
            String processingStage = "";
            try
            {
                // enable these for debugging
                //Console.ForegroundColor = ConsoleColor.DarkMagenta;
                //Console.WriteLine("Thread {0} : Event Name : {1}  ", Thread.CurrentThread.ManagedThreadId, in_x_event.Name);
                //Console.ResetColor();

                // find out which data table we need to work on based on the event name
                processingStage = "Lookup DataTable for Event";
                DataTable dt_To_Populate = m_event_tables[in_x_event.Name];
                DataRow row = dt_To_Populate.NewRow();

                //extract pieces of information from the event fields and actions
                processingStage = "Extract all fields from Event";
                retVal = GetDataFromEvent(row, in_x_event);
                if (true == retVal)
                    throw new InvalidDataException("Error processing event information fields");

                // add the complete row to the data table
                processingStage = "Add event information to DataTable";
                dt_To_Populate.Rows.Add(row);

                // now check the data table size against batch size and save the data table to sql database
                if (XELoader.FileProcessor.myInputParameters.m_bulkcopy_BatchSize <= dt_To_Populate.Rows.Count)
                {
                    processingStage = "Saving DataTable to Database";
                    SaveDataTableToSQLDatabase(dt_To_Populate, "BatchSize Reached");
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : ***  Exception encountered  *** ", Thread.CurrentThread.ManagedThreadId);
                Console.WriteLine("Thread {0} : Processing Stage  : {1}  ", Thread.CurrentThread.ManagedThreadId, processingStage);
                Console.WriteLine("Thread {0} : Event Name        : {1}  ", Thread.CurrentThread.ManagedThreadId, in_x_event.Name);
                Console.WriteLine("Thread {0} : Event Timestamp   : {1}  ", Thread.CurrentThread.ManagedThreadId, in_x_event.Timestamp);

                Console.WriteLine("Thread {0} : Exception message : {1}  ", Thread.CurrentThread.ManagedThreadId, e.Message);
                Console.WriteLine("Thread {0} : Exception code    : {1}  ", Thread.CurrentThread.ManagedThreadId, System.Runtime.InteropServices.Marshal.GetHRForException(e));   //Using e.HREsult gives CS0122 in new versions of .NET
                Console.WriteLine("Thread {0} : Exception stack   : {1}  ", Thread.CurrentThread.ManagedThreadId, e.StackTrace);

                Console.ResetColor();

                m_Number_of_Errors++;                                                       // local error count for this file
                if (m_Number_of_Errors >= XELoader.FileProcessor.myInputParameters.m_errorCount)
                    throw e;
            }
        }

        public bool GetDataFromEvent(DataRow in_datarow, PublishedEvent in_x_event)
        {
            bool bError = false;
            try
            {
                // populate the file id from which this event is coming
                in_datarow["e_Imported_File_Id"] = m_fileID;
                // populate the mandatory fields
                // first populate the row with timestamp from the event
                in_datarow["e_Time_Of_Event"] = in_x_event.Timestamp.DateTime;
                in_datarow["e_Time_Of_Event_utc"] = in_x_event.Timestamp.DateTime;
                // perform conversion to local time zone
                in_datarow["e_Time_Of_Event_local"]
                    = ("Local" == XELoader.FileProcessor.myInputParameters.m_timeZone)
                    ? in_x_event.Timestamp.LocalDateTime
                    : TimeZoneInfo.ConvertTimeFromUtc(in_x_event.Timestamp.DateTime, XELoader.FileProcessor.myInputParameters.m_timeZoneInfo);
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : ***  Exception encountered  while extracting time fields from event *** ", Thread.CurrentThread.ManagedThreadId);
                Console.WriteLine("Thread {0} : Exception message : {1}  ", Thread.CurrentThread.ManagedThreadId, e.Message);
                Console.ResetColor();
                bError = true;
            }
            // Iterate through each data field from the event and populate the row
            try
            {
                foreach (PublishedEventField in_x_event_field in in_x_event.Fields)
                {
                    // enable this for debugging
                    //Console.ForegroundColor = ConsoleColor.DarkMagenta;
                    //Console.WriteLine("Thread {0} : Data Field Name : {1}  : Data Field Type : {2}", Thread.CurrentThread.ManagedThreadId, in_x_event_field.Name, in_x_event_field.Type.ToString());
                    //Console.ResetColor();

                    //special processing for different fields
                    switch (in_x_event_field.Type.ToString())
                    {
                        case "Microsoft.SqlServer.XEvent.MapValue":
                        case "Microsoft.SqlServer.XEvent.ActivityId":
                            {
                                in_datarow["c_" + in_x_event_field.Name] = ExtractStringFromStringAfterTruncation(in_x_event_field.Value.ToString());
                                break;
                            }
                        case "System.Byte[]":
                            {
                                switch (in_x_event_field.Name)
                                {
                                    case "data_stream":         // we need to figure out if we need to import data_stream from rpc events since we cannot read them without a decoder, so skipping for now
                                        break;
                                    default:
                                        {
                                            in_datarow["c_" + in_x_event_field.Name] = ExtractBinaryFromBinaryAfterTruncation((byte[])in_x_event_field.Value);
                                            break;
                                        }
                                }
                                break;
                            }
                        case "System.String":
                            {
                                // we did special processing for some event names, but now in recent builds, doing this generic to avoid any overflows
                                // case "statement": case "batch_text": case "data": case "message": case "execution_statistics":
                                in_datarow["c_" + in_x_event_field.Name] = ExtractStringFromStringAfterTruncation(in_x_event_field.Value.ToString());
                                break;
                            }
                        case "Microsoft.SqlServer.XEvent.XMLData":
                            {
                                // special handling for bug in event data
                                // in sql 2012 and 2014 module_end event has statement data field declared as xml but is is a pure text value
                                if ((("module_end" == in_x_event.Name) || ("module_start" == in_x_event.Name)) & ("statement" == in_x_event_field.Name))
                                {
                                    in_datarow["c_" + in_x_event_field.Name] = "";
                                }
                                else    // everyone else follows the usual rule
                                {
                                    in_datarow["c_" + in_x_event_field.Name] = ExtractStringFromXMLAfterTruncation(in_x_event_field.Value.ToString());
                                }
                                break;
                            }
                        default:
                            {
                                in_datarow["c_" + in_x_event_field.Name] = in_x_event_field.Value;
                                break;
                            }
                    }
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : ***  Exception encountered  while extracting data fields/columns from event *** ", Thread.CurrentThread.ManagedThreadId);
                Console.WriteLine("Thread {0} : Exception message : {1}  ", Thread.CurrentThread.ManagedThreadId, e.Message);
                Console.ResetColor();
                bError = true;
            }
            // Iterate through each action from the event and populate the row
            try
            {
                foreach (PublishedAction in_x_event_action in in_x_event.Actions)
                {
                    // enable this to debug
                    //Console.ForegroundColor = ConsoleColor.DarkMagenta;
                    //Console.WriteLine("Thread {0} : Action Name : {1}  : Action Type : {2}", Thread.CurrentThread.ManagedThreadId, in_x_event_action.Name, in_x_event_action.Type.ToString());
                    //Console.ResetColor();

                    switch (in_x_event_action.Type.ToString())
                    {
                        case "Microsoft.SqlServer.XEvent.MapValue":
                        case "Microsoft.SqlServer.XEvent.ActivityId":
                            {
                                in_datarow["a_" + in_x_event_action.Name] = in_x_event_action.Value.ToString();
                                break;
                            }
                        case "System.Byte[]":
                            {
                                switch (in_x_event_action.Name)
                                {
                                    // special handling for query hash and plan hash to create the handle values in binary as well as integer
                                    // this is required to be able to match with DMV captures which are in binary format
                                    case "query_hash":
                                    case "query_plan_hash":
                                        {
                                            in_datarow["a_" + in_x_event_action.Name] = in_x_event_action.Value;
                                            UInt64 hash_value = Convert.ToUInt64(in_x_event_action.Value);
                                            in_datarow["a_" + in_x_event_action.Name + "_bin"] = BitConverter.GetBytes(hash_value).Reverse().ToArray();
                                            break;
                                        }
                                    // for callstack action, we break the sequence into individual frames and add the debugger command to make it ready for use in dbg
                                    case "callstack":
                                        {
                                            in_datarow["a_" + in_x_event_action.Name] = ExtractBinaryFromBinaryAfterTruncation((byte[])in_x_event_action.Value);
                                            in_datarow["a_" + in_x_event_action.Name + "_debugcmd"] = GenerateDebuggerCommandFromCallStackAction((byte[])in_x_event_action.Value);
                                            break;
                                        }
                                    default:
                                        {
                                            in_datarow["a_" + in_x_event_action.Name] = ExtractBinaryFromBinaryAfterTruncation((byte[])in_x_event_action.Value);
                                            break;
                                        }
                                }
                                break;
                            }
                        case "System.String":
                            {
                                // we did special processing for some event names, but now in recent builds, doing this generic to avoid any overflows
                                // case "sql_text":
                                in_datarow["a_" + in_x_event_action.Name] = ExtractStringFromStringAfterTruncation(in_x_event_action.Value.ToString());
                                break;
                            }
                        case "Microsoft.SqlServer.XEvent.XMLData":
                            {
                                in_datarow["a_" + in_x_event_action.Name] = ExtractStringFromXMLAfterTruncation(in_x_event_action.Value.ToString());
                                break;
                            }
                        default:
                            {
                                in_datarow["a_" + in_x_event_action.Name] = in_x_event_action.Value;
                                break;
                            }
                    }
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : ***  Exception encountered  while extracting actions from event *** ", Thread.CurrentThread.ManagedThreadId);
                Console.WriteLine("Thread {0} : Exception message : {1}  ", Thread.CurrentThread.ManagedThreadId, e.Message);
                Console.ResetColor();
                bError = true;
            }

            return bError;
        }

        public String ExtractStringFromStringAfterTruncation(String in_String)
        {
            // Columnstore does not support max data types in SQL 2014 and 2016, so we need to perform truncation of large strings
            if (("ColumnStore" == XELoader.FileProcessor.myInputParameters.m_IndexType) && (false == XELoader.FileProcessor.myInputParameters.m_LOBallowedonCSI))
            {
                // storing incoming string length since we reuse it multiple times
                int _length = in_String.Length;
                // is the string longer than what is specified in the program input requirements
                if (XELoader.FileProcessor.myInputParameters.m_StringToStringTruncation < _length)
                {
                    _length = XELoader.FileProcessor.myInputParameters.m_StringToStringTruncation;
                    m_Number_of_String_Truncations++;
                }
                return in_String.Substring(0, _length);
            }
            else
            {
                // retain original string if this is rowstore since we do not want to truncate at expense of speed and compression
                return in_String;
            }
        }

        public String ExtractStringFromXMLAfterTruncation(String in_String)
        {
            // Columnstore does not support max data types in SQL 2014 and 2016, so we need to perform truncation of large strings
            if (("ColumnStore" == XELoader.FileProcessor.myInputParameters.m_IndexType) && (false == XELoader.FileProcessor.myInputParameters.m_LOBallowedonCSI))
            {
                // storing incoming string length since we reuse it multiple times
                int _length = in_String.Length;
                // is the string longer than what is specified in the program input requirements
                if (XELoader.FileProcessor.myInputParameters.m_XMLToStringTruncation < _length)
                {
                    _length = XELoader.FileProcessor.myInputParameters.m_XMLToStringTruncation;
                    m_Number_of_XML_Truncations++;
                }
                return in_String.Substring(0, _length);
            }
            else
            {
                // retain original string if this is rowstore since we do not want to truncate at expense of speed and compression
                return in_String;
            }
        }

        public byte[] ExtractBinaryFromBinaryAfterTruncation(byte[] in_Binary)
        {
            // Columnstore does not support max data types in SQL 2014 and 2016, so we need to perform truncation of large strings
            if (("ColumnStore" == XELoader.FileProcessor.myInputParameters.m_IndexType) && (false == XELoader.FileProcessor.myInputParameters.m_LOBallowedonCSI))
            {
                // storing incoming string length since we reuse it multiple times
                int _length = in_Binary.Length;
                // is the string longer than what is specified in the program input requirements
                if (XELoader.FileProcessor.myInputParameters.m_BinaryToBinaryTruncation < _length)
                {
                    _length = XELoader.FileProcessor.myInputParameters.m_StringToStringTruncation;
                    m_Number_of_Binary_Truncations++;
                }
                byte[] out_Binary = new byte[_length];
                Array.Copy(in_Binary, out_Binary, _length);
                return out_Binary;
            }
            else
            {
                // retain original string if this is rowstore since we do not want to truncate at expense of speed and compression
                return in_Binary;
            }
        }

        public String GenerateDebuggerCommandFromCallStackAction(byte[] CallStack)
        {
            int LengthOfCallStack = CallStack.Length;
            int FrameLength = XELoader.FileProcessor.myInputParameters.m_CallStack_FrameLength;
            byte[] DestinationBytes = new byte[FrameLength];
            int NumFrames = LengthOfCallStack / FrameLength;
            int iSourceIndex = 0;
            String debuggerCommand = "";
            for (int iFrame = 0; iFrame < NumFrames; iFrame++)
            {
                Array.Copy((byte[])CallStack, iSourceIndex, DestinationBytes, 0, FrameLength);
                Array.Reverse(DestinationBytes);
                String bitString = BitConverter.ToString(DestinationBytes);
                debuggerCommand += XELoader.FileProcessor.myInputParameters.m_CallStack_command + " " + bitString.Replace("-", "") + ";";
                iSourceIndex += FrameLength;
            }
            return debuggerCommand;
        }

        public void SaveDataTableToSQLDatabase(DataTable in_dt_To_SaveToDatabase, String ReasonForFlush)
        {
            if (0 < in_dt_To_SaveToDatabase.Rows.Count)
            {
                Console.WriteLine("Thread {0} : Perform bulk copy to Database : {1} : Flushing rows : {2} : for DataTable : {3} ", Thread.CurrentThread.ManagedThreadId, ReasonForFlush, in_dt_To_SaveToDatabase.Rows.Count, in_dt_To_SaveToDatabase.TableName);

                // Establish a connection to the SQL Server where the parsed data will be stored
                SqlConnection DestinationConnection = new SqlConnection(XELoader.FileProcessor.myInputParameters.m_ConnectionString);
                DestinationConnection.Open();
                // Setup the bulk copy context and associations
                SqlBulkCopy DestinationBulkCopy = new SqlBulkCopy(DestinationConnection);
                DestinationBulkCopy.DestinationTableName = "[" + XELoader.FileProcessor.myInputParameters.m_SchemaName + "].[" + in_dt_To_SaveToDatabase.TableName + "]";
                DestinationBulkCopy.BatchSize = XELoader.FileProcessor.myInputParameters.m_bulkcopy_BatchSize;
                DestinationBulkCopy.BulkCopyTimeout = 0;
                // establish the column mappings
                try
                {
                    foreach (DataColumn column in in_dt_To_SaveToDatabase.Columns)
                    {
                        SqlBulkCopyColumnMapping columnmap = new SqlBulkCopyColumnMapping(column.ColumnName, column.ColumnName);
                        DestinationBulkCopy.ColumnMappings.Add(columnmap);
                    }
                }
                catch (SqlException ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Thread {1} : Exception encountered while performing column mapping for table {0}", in_dt_To_SaveToDatabase.TableName, Thread.CurrentThread.ManagedThreadId);
                    Console.WriteLine("Thread {0} : Please ensure the table exists in the database with the expected schema", Thread.CurrentThread.ManagedThreadId);
                    Console.WriteLine("Thread {0} : Exception # {1}, message : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                    Console.WriteLine("{0}", ex.StackTrace);
                    Console.ResetColor();
                }
                DateTime BulkCopyStartTime = DateTime.Now;
                // Transfer the data from the data table to the SQL Database
                try
                {
                    DestinationBulkCopy.WriteToServer(in_dt_To_SaveToDatabase);
                }
                catch (SqlException ex)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Thread {1} : Exception encountered while performing bulk copy for table {0}", in_dt_To_SaveToDatabase.TableName, Thread.CurrentThread.ManagedThreadId);
                    Console.WriteLine("Thread {0} : Please ensure the table exists in the database with the expected schema", Thread.CurrentThread.ManagedThreadId);
                    Console.WriteLine("Thread {0} : Exception # {1}, message : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                    Console.WriteLine("{0}", ex.StackTrace);
                    Console.ResetColor();
                }
                DateTime BulkCopyEndTime = DateTime.Now;
                //track the bulk copy timings
                m_Bulk_Copy_Duration += (BulkCopyEndTime - BulkCopyStartTime);
                //increment the count of events in the status tracker
                XELoader.FileProcessor.myTrackStatus.m_Number_of_Events += in_dt_To_SaveToDatabase.Rows.Count;

                // cleanup all resources
                DestinationBulkCopy.Close();
                DestinationConnection.Close();

            }
            else
                Console.WriteLine("Thread {0} : Skipped bulk copy to Database : {1} : Flushing Rows : {2} : for DataTable : {3} ", Thread.CurrentThread.ManagedThreadId, ReasonForFlush, in_dt_To_SaveToDatabase.Rows.Count, in_dt_To_SaveToDatabase.TableName);

            // cleanup all resources
            in_dt_To_SaveToDatabase.Clear();
        }

        public void SaveAllDataTablesToSQLDatabase()
        {
            foreach (DataTable dt_To_SaveToDatabase in m_event_tables)
                SaveDataTableToSQLDatabase(dt_To_SaveToDatabase, "Residual Flush");
        }

        public void ClearAllDataTables()
        {
            m_data_set.Tables.Clear();
        }

        public Int64 InsertFileInfoIntoTrackingTable()
        {
            // Establish a connection to the SQL Server where the parsed data will be stored
            SqlConnection DestinationConnection = new SqlConnection(XELoader.FileProcessor.myInputParameters.m_ConnectionString);
            DestinationConnection.Open();
            // form the insert statement
            Int64 file_id = 0;
            String tsql_Insert_tbl_ImportedXEventFiles = "insert into [dbo].[tbl_ImportedXEventFiles] ([file_folder],[file_name]) values (@DirName, @FileName); SELECT CAST(scope_identity() AS bigint)";
            SqlCommand sql_cmd_Insert_tbl_ImportedXEventFiles = new SqlCommand(tsql_Insert_tbl_ImportedXEventFiles, DestinationConnection);
            sql_cmd_Insert_tbl_ImportedXEventFiles.Parameters.Add("@DirName", SqlDbType.NVarChar);
            sql_cmd_Insert_tbl_ImportedXEventFiles.Parameters["@DirName"].Value = m_event_File_Info.DirectoryName;
            sql_cmd_Insert_tbl_ImportedXEventFiles.Parameters.Add("@FileName", SqlDbType.NVarChar);
            sql_cmd_Insert_tbl_ImportedXEventFiles.Parameters["@FileName"].Value = m_event_File_Info.Name;
            // store information about this file in the table and get its id
            try
            {
                file_id = (Int64)sql_cmd_Insert_tbl_ImportedXEventFiles.ExecuteScalar();
            }
            catch (SqlException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                Console.ResetColor();
            }
            DestinationConnection.Close();
            return file_id;
        }

        public void UpdateFileInfoInTrackingTable()
        {
            // Establish a connection to the SQL Server where the information will be stored
            SqlConnection DestinationConnection = new SqlConnection(XELoader.FileProcessor.myInputParameters.m_ConnectionString);
            DestinationConnection.Open();
            // form the update statement
            String tsql_Insert_tbl_ImportedXEventFiles = "update [dbo].[tbl_ImportedXEventFiles] set [total_processing_time] = @total_processing_time, [bulk_copy_time] = @bulk_copy_time where [file_id] = @file_id;";
            SqlCommand sql_cmd_Insert_tbl_ImportedXEventFiles = new SqlCommand(tsql_Insert_tbl_ImportedXEventFiles, DestinationConnection);
            // establish the parameters
            sql_cmd_Insert_tbl_ImportedXEventFiles.Parameters.Add("@file_id", SqlDbType.BigInt);
            sql_cmd_Insert_tbl_ImportedXEventFiles.Parameters["@file_id"].Value = m_fileID;
            sql_cmd_Insert_tbl_ImportedXEventFiles.Parameters.Add("@total_processing_time", SqlDbType.Time);
            sql_cmd_Insert_tbl_ImportedXEventFiles.Parameters["@total_processing_time"].Value = m_Total_Processing_Duration;
            sql_cmd_Insert_tbl_ImportedXEventFiles.Parameters.Add("@bulk_copy_time", SqlDbType.Time);
            sql_cmd_Insert_tbl_ImportedXEventFiles.Parameters["@bulk_copy_time"].Value = m_Bulk_Copy_Duration;

            // store information about this file in the table and get its id
            try
            {
                sql_cmd_Insert_tbl_ImportedXEventFiles.ExecuteScalar();
            }
            catch (SqlException ex)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : Msg : {1} : {2}", Thread.CurrentThread.ManagedThreadId, ex.Number, ex.Message);
                Console.ResetColor();
            }
            // cleanup resources
            DestinationConnection.Close();
        }
    }
}
