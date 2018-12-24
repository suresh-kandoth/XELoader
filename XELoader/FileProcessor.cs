using System;
using System.Reflection;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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
    public class FileProcessor
    {
        public static TrackStatus myTrackStatus;
        public static InputParameters myInputParameters;
        public static EventMetadata myEventMetadata;

        static int Main(string[] args)
        {
            try
            {
                PrintProgramDetails();
                // lets first check if we have the required assemblies installed on this system
                bool a_result = CheckAssembly();
                if (false == a_result)
                    return 0;

                myTrackStatus = new TrackStatus();
                myInputParameters = new InputParameters();
                bool status_of_params = myInputParameters.ProcessInputParameters(args);
                if (true == status_of_params)
                {
                    myInputParameters.DetectServerCapabilities();
                    // we do not need to create the database and schema if we are in append mode
                    if (false == myInputParameters.m_AppendToExistingData)
                    {
                        myInputParameters.CreateDatabase();
                        myInputParameters.CreateSchema();
                        // create the table to track files loaded
                        myInputParameters.CreateTrackingTable();
                        myTrackStatus.IsTableCreated = false;
                    }
                    else
                    {
                        myTrackStatus.IsTableCreated = true;
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine("Thread {0} : Skipping database and schema creation as requested by parameter combinations [See help for -w, -c, -a]", Thread.CurrentThread.ManagedThreadId);
                        Console.ResetColor();
                    }
                    // when we process the first file we will initialize the metadata, so we declare this here
                    myEventMetadata = new EventMetadata();

                    // Now we are ready to process the files, create metadata and then load the event data
                    ProcessFiles();

                    // print final stats
                    myTrackStatus.PrintStatistics();
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : ***  Exception encountered  *** ", Thread.CurrentThread.ManagedThreadId);
                Console.WriteLine("Thread {0} : Exception message : {1}  ", Thread.CurrentThread.ManagedThreadId, e.Message);
                Console.WriteLine("Thread {0} : Exception code : {1}  ", Thread.CurrentThread.ManagedThreadId, System.Runtime.InteropServices.Marshal.GetHRForException(e));  //Using e.HResult gives CS0122 in new versions of .NET
                Console.WriteLine("Thread {0} : Exception stack : {1}  ", Thread.CurrentThread.ManagedThreadId, e.StackTrace);
                Console.ResetColor();

                return 1;
            }
        }

        static void ProcessFiles()
        {
            // special processing logic for old file format to use xel and xem file sets
            if ("" != myInputParameters.m_XEM_File_To_Process)
            {
                ProcessXELwithXEM();
                return;
            }
            //we have a directory of files to process
            if ("" == myInputParameters.m_XEL_File_To_Process)
            {
                //process the directory
                if (Directory.Exists(myInputParameters.m_XE_Directory_To_Process))
                {
                    string[] xel_Files_To_Process = Directory.GetFiles(myInputParameters.m_XE_Directory_To_Process, myInputParameters.m_XEL_File_Pattern);
                    Console.WriteLine("Thread {0} : Detected {1} file(s) in the input directory : [ {2} ]", Thread.CurrentThread.ManagedThreadId, xel_Files_To_Process.Length, myInputParameters.m_XE_Directory_To_Process);

                    // establish the file pattern we are going to process
                    if (("*.xel" == myInputParameters.m_XEL_File_Pattern) & ("" == myTrackStatus.m_filePatternInUse))
                    {
                        FileInfo current_File = new FileInfo(xel_Files_To_Process[0]);                  // get the first file from the directory
                        myTrackStatus.m_filePatternInUse = GetLeadingFilePattern(current_File.Name);
                        Console.ForegroundColor = ConsoleColor.DarkYellow;
                        Console.WriteLine("Thread {0} : Established file name pattern to import : {1}_*.xel   [Overide with -p for alternate pattern]", Thread.CurrentThread.ManagedThreadId, myTrackStatus.m_filePatternInUse);
                        Console.ResetColor();
                    }
                    else
                    {
                        myTrackStatus.m_filePatternInUse = myInputParameters.m_XEL_File_Pattern;
                    }

                    if (myInputParameters.m_NumThreads > 1)
                    {
                        //process directory using parallel threads
                        ParallelOptions pOptions = new ParallelOptions();
                        pOptions.MaxDegreeOfParallelism = Math.Min(myInputParameters.m_NumThreads, xel_Files_To_Process.Length);
                        Console.WriteLine("Thread {0} : Using {1} thread(s) to process the event files [Override with -t parameter]", Thread.CurrentThread.ManagedThreadId, pOptions.MaxDegreeOfParallelism);
                        Parallel.ForEach(xel_Files_To_Process, pOptions, xel_File_To_Process => ProcessOneFile(xel_File_To_Process));
                    }
                    else
                    {
                        Console.WriteLine("Thread {0} : Using single thread to process the event data ", Thread.CurrentThread.ManagedThreadId);
                        //process directory serially using single thread
                        foreach (string xel_File_To_Process in xel_Files_To_Process)
                        {
                            ProcessOneFile(xel_File_To_Process);
                        }
                    }
                }
            }
            // we have only a single file to process
            else
            {
                //save the file pattern we are processing
                FileInfo file_to_process = new FileInfo(myInputParameters.m_XEL_File_To_Process);
                myTrackStatus.m_filePatternInUse = GetLeadingFilePattern(file_to_process.Name);
                //process the single file
                ProcessOneFile(myInputParameters.m_XEL_File_To_Process);
            }
        }

        static void ProcessOneFile(String in_file_to_process)
        {
            FileInfo current_File = new FileInfo(in_file_to_process);
            String current_File_Pattern = GetLeadingFilePattern(current_File.Name);

            // did the user request a specific pattern
            if ("*.xel" != myInputParameters.m_XEL_File_Pattern)
            {
                String pattern = "*.xel";
                int position = myInputParameters.m_XEL_File_Pattern.LastIndexOf(pattern);
                String requested_leading_pattern = myInputParameters.m_XEL_File_Pattern.Substring(0, position);

                String current_leading_pattern = current_File_Pattern.Substring(0, requested_leading_pattern.Length);
                if (current_leading_pattern != requested_leading_pattern)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Thread {0} : Skipping file since pattern [{1}] did not match : {2} [Overide with -p for alternate pattern]", Thread.CurrentThread.ManagedThreadId, requested_leading_pattern, current_File.Name);
                    Console.ResetColor();
                    return;
                }
            }
            // check if this file belongs to the same XE session or something else
            else if (current_File_Pattern != myTrackStatus.m_filePatternInUse)
            {
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Thread {0} : Skipping file since pattern [{1}] did not match : {2}  [Overide with -p for alternate pattern]", Thread.CurrentThread.ManagedThreadId, myTrackStatus.m_filePatternInUse, current_File.Name);
                Console.ResetColor();
                return;
            }

            // report the pattern match status
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("Thread {0} : Requested pattern [{1}] matches file name : {2}", Thread.CurrentThread.ManagedThreadId, myTrackStatus.m_filePatternInUse, current_File.Name);
            Console.ResetColor();

            EventHolder x_event_holder = null;
            try
            {
                //increment file count in the tracker
                myTrackStatus.m_Number_of_Files++;

                // call read ahead on this file to speed up the XE Linq reader
                if ("y" == myInputParameters.m_ReadAhead)
                {
                    ReadAhead x_ReadAhead = new ReadAhead(in_file_to_process);
                    Thread x_Thread = new Thread(new ThreadStart(x_ReadAhead.DoReadAhead));
                    x_Thread.Start();
                }

                //check if the file exists
                if (File.Exists(in_file_to_process))
                {
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine("Thread {0} : Start processing file : {1}", Thread.CurrentThread.ManagedThreadId, current_File.Name);
                    Console.ResetColor();

                    QueryableXEventData x_event_file;
                    //open the XEL file
                    try
                    {
                        x_event_file = new QueryableXEventData(in_file_to_process);
                    }
                    catch (Exception e)
                    {
                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine("Thread {0} : ***  Exception encountered calling QueryableXEventData in ProcessOneFile *** ", Thread.CurrentThread.ManagedThreadId);
                        Console.WriteLine("Thread {0} : Exception message : {1}  ", Thread.CurrentThread.ManagedThreadId, e.Message);
                        Console.WriteLine("Thread {0} : Exception code : {1}  ", Thread.CurrentThread.ManagedThreadId, System.Runtime.InteropServices.Marshal.GetHRForException(e));  //Using e.HResult gives CS0122 in new versions of .NET
                        Console.WriteLine("Thread {0} : Exception stack : {1}  ", Thread.CurrentThread.ManagedThreadId, e.StackTrace);
                        Console.ResetColor();

                        return;
                    }
                    // process the metadata from the XEL file and add information to the data tables
                    // need to do this only one time, so need to sync with other file processors
                    myTrackStatus.acquire_metadata_Lock(true);
                    {
                        if (false == myEventMetadata.m_dt_Initialized)
                        {
                            myEventMetadata.ExtractMetadataFromFile(x_event_file);
                            myEventMetadata.m_dt_Initialized = true;
                            myTrackStatus.IsTableCreated = true;
                        }
                    }
                    myTrackStatus.release_metadata_Lock(true);

                    x_event_holder = new EventHolder(in_file_to_process);
                    // save information about this file into the tracking table 
                    // get the identity value back to use as default value in the data table
                    x_event_holder.m_fileID = x_event_holder.InsertFileInfoIntoTrackingTable();
                    DateTime start_Time = DateTime.Now;
                    // now loop through each event in the file, extract information and store it in the data table
                    Console.WriteLine("Thread {0} : Extracting events from the extended event file : {1}", Thread.CurrentThread.ManagedThreadId, current_File.Name);
                    foreach (PublishedEvent x_event in x_event_file)
                    {
                        x_event_holder.ProcessEvent(x_event);
                    }   //foreach loop
                    // Now we can commit left over in the data tables to the database
                    x_event_holder.SaveAllDataTablesToSQLDatabase();
                    x_event_holder.ClearAllDataTables();

                    DateTime end_Time = DateTime.Now;
                    x_event_holder.m_Total_Processing_Duration = end_Time - start_Time;
                    // update status to console
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine("Thread {0} : Finished processing file {1} , duration : {2} , bulk copy duration : {3} ",
                        Thread.CurrentThread.ManagedThreadId, x_event_holder.m_event_File_Info.Name, x_event_holder.m_Total_Processing_Duration.ToString(), x_event_holder.m_Bulk_Copy_Duration.ToString());
                    Console.ResetColor();
                    // save runtime for each file in the backend table
                    x_event_holder.UpdateFileInfoInTrackingTable();
                    // increment global error and truncation counts from this file
                    if (myTrackStatus.m_Number_of_Errors > 0 || myTrackStatus.m_Number_of_StringTruncations > 0 || myTrackStatus.m_Number_of_XMLTruncations > 0 || myTrackStatus.m_Number_of_BinaryTruncations > 0)
                    {
                        myTrackStatus.acquire_error_truncation_Lock(true);
                        {
                            myTrackStatus.m_Number_of_Errors += x_event_holder.m_Number_of_Errors;
                            myTrackStatus.m_Number_of_StringTruncations += x_event_holder.m_Number_of_String_Truncations;
                            myTrackStatus.m_Number_of_XMLTruncations += x_event_holder.m_Number_of_XML_Truncations;
                            myTrackStatus.m_Number_of_BinaryTruncations += x_event_holder.m_Number_of_Binary_Truncations;
                        }
                        myTrackStatus.release_error_truncation_Lock(true);
                    }
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : ***  Exception encountered  *** ", Thread.CurrentThread.ManagedThreadId);
                Console.WriteLine("Thread {0} : Stop processing file : {1}  ", Thread.CurrentThread.ManagedThreadId, current_File.Name);
                Console.WriteLine("Thread {0} : Exception message    : {1}  ", Thread.CurrentThread.ManagedThreadId, e.Message);
                Console.WriteLine("Thread {0} : Exception code       : {1}  ", Thread.CurrentThread.ManagedThreadId, System.Runtime.InteropServices.Marshal.GetHRForException(e));    //just using e.HResult gives CS0122 in new versions of .NET
                Console.WriteLine("Thread {0} : Exception stack      : {1}  ", Thread.CurrentThread.ManagedThreadId, e.StackTrace);
                Console.ResetColor();

                // increment global error and truncation counts from this file
                myTrackStatus.acquire_error_truncation_Lock(true);
                {
                    myTrackStatus.m_Number_of_Errors += x_event_holder.m_Number_of_Errors;
                    myTrackStatus.m_Number_of_StringTruncations += x_event_holder.m_Number_of_String_Truncations;
                    myTrackStatus.m_Number_of_XMLTruncations += x_event_holder.m_Number_of_XML_Truncations;
                    myTrackStatus.m_Number_of_BinaryTruncations += x_event_holder.m_Number_of_Binary_Truncations;
                }
                myTrackStatus.release_error_truncation_Lock(true);
            }
        }

        static void ProcessXELwithXEM()
        {
            //increment file count in the tracker
            myTrackStatus.m_Number_of_Files++;

            EventHolder x_event_holder;
            x_event_holder = new EventHolder(myInputParameters.m_XEL_File_To_Process);
            //check if the file exists
            if (File.Exists(myInputParameters.m_XEL_File_To_Process))
            {
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("Thread {0} : Start processing file : {1}", Thread.CurrentThread.ManagedThreadId, x_event_holder.m_event_File_Info.Name);
                Console.ResetColor();
                // save information about this file into the tracking table 
                // get the identity value back to use as default value in the data table
                x_event_holder.m_fileID = x_event_holder.InsertFileInfoIntoTrackingTable();
                DateTime start_Time = DateTime.Now;
                //open the XEL file along with the XEM file
                String[] xelFiles = new String[1];
                xelFiles[0] = myInputParameters.m_XEL_File_To_Process;
                String[] xemFiles = new String[1];
                xemFiles[0] = myInputParameters.m_XEM_File_To_Process;
                QueryableXEventData x_event_file = new QueryableXEventData(xelFiles, xemFiles);
                // process the metadata from the XEL file and add information to the data tables
                // need to do this only one time, so need to sync with other file processors
                myTrackStatus.acquire_metadata_Lock(true);
                {
                    if (false == myEventMetadata.m_dt_Initialized)
                    {
                        myEventMetadata.ExtractMetadataFromFile(x_event_file);
                        myEventMetadata.m_dt_Initialized = true;
                        XELoader.FileProcessor.myTrackStatus.IsTableCreated = true;
                    }
                }
                myTrackStatus.release_metadata_Lock(true);
                // now loop through each event in the file, extract information and store it in the data table
                foreach (PublishedEvent x_event in x_event_file)
                {
                    x_event_holder.ProcessEvent(x_event);
                }   //foreach loop
                // Now we can commit left over in the data tables to the database
                x_event_holder.SaveAllDataTablesToSQLDatabase();
                x_event_holder.ClearAllDataTables();

                DateTime end_Time = DateTime.Now;
                x_event_holder.m_Total_Processing_Duration = end_Time - start_Time;
                // update status to console
                Console.ForegroundColor = ConsoleColor.Cyan;
                Console.WriteLine("Thread {0} : Finished processing file {1} , duration : {2} , bulk copy duration : {3} ",
                    Thread.CurrentThread.ManagedThreadId, x_event_holder.m_event_File_Info.Name, x_event_holder.m_Total_Processing_Duration.ToString(), x_event_holder.m_Bulk_Copy_Duration.ToString());
                Console.ResetColor();
                // save runtime for each file in the backend table
                x_event_holder.UpdateFileInfoInTrackingTable();
            }
        }

        static String GetLeadingFilePattern(String FileProcessed)
        {
            String strLeadingPattern = "";
            String pattern = "_";

            //special case for single file processing
            if ("" != myInputParameters.m_XEL_File_To_Process)
            {
                //strip the extension part and use the single file as the pattern
                String fileextension = ".xel";
                int position_extn = FileProcessed.LastIndexOf(fileextension);
                strLeadingPattern = FileProcessed.Substring(0, position_extn);
                return strLeadingPattern;
            }

            // file names are of the format: SessionName_PartitionID_TimeStampInfo.xel
            int position = FileProcessed.LastIndexOf(pattern);
            // we run into issues if the file name does not follow the format we expect
            if (0 > position)
                throw new System.ArgumentException("File name is not in the expected format: SessionName_PartitionID_TimeStampInfo.xel ", "FileProcessed");
            // we got the first part right
            strLeadingPattern = FileProcessed.Substring(0, position);
            position = strLeadingPattern.LastIndexOf(pattern);
            // we run into issues if the file name does not follow the format we expect
            if (0 > position)
                throw new System.ArgumentException("File name is not in the expected format: SessionName_PartitionID_TimeStampInfo.xel ", "FileProcessed");
            // we got the second part right
            strLeadingPattern = strLeadingPattern.Substring(0, position);
            // we run into issues if the file name does not follow the format we expect
            if (0 > strLeadingPattern.Length)
                throw new System.ArgumentException("File name is not in the expected format: SessionName_PartitionID_TimeStampInfo.xel ", "FileProcessed");

            return strLeadingPattern;
        }

        static bool CheckAssembly()
        {
            String XELinq = "Microsoft.SqlServer.XEvent.Linq, Version=14.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91, processor architecture=AMD64";
            var an2 = new AssemblyName(XELinq);
            try
            {
                var assem = Assembly.Load(an2);
                Console.WriteLine("Loaded assembly: {0}", assem.Location);
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Cannot start execution; {0}", e.Message);
                Console.WriteLine(" ");
                Console.WriteLine("Locate the file Microsoft.SqlServer.XEvent.Linq.dll with version 14.x.x.x in one of the following locations:");
                Console.WriteLine("   C:\\Program Files\\Microsoft SQL Server\\140\\Shared\\");
                Console.WriteLine("   C:\\Program Files\\Microsoft SQL Server\\MSSQL14.INSTID\\MSSQL\\Binn\\");
                Console.WriteLine("   C:\\Program Files (x86)\\Microsoft SQL Server\\140\\Tools\\Binn\\ManagementStudio\\");
                Console.WriteLine("Copy this file Microsoft.SqlServer.XEvent.Linq.dll to the folder where XELoader.exe is located and restart this program.");
                Console.ResetColor();
                return false;
            }

            String XECore = "Microsoft.SqlServer.XE.Core, Version=14.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91, processor architecture=AMD64";
            var an1 = new AssemblyName(XECore);
            try
            {
                var assem = Assembly.Load(an1);
                Console.WriteLine("Loaded assembly: {0}:", assem.Location);
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Cannot start execution; {0}:", e.Message);
                Console.WriteLine(" ");
                Console.WriteLine("Locate the file Microsoft.SqlServer.XE.Core with version 14.x.x.x in one of the following locations:");
                Console.WriteLine("   C:\\Program Files\\Microsoft SQL Server\\140\\Shared\\");
                Console.WriteLine("   C:\\Program Files\\Microsoft SQL Server\\MSSQL14.INSTID\\MSSQL\\Binn\\");
                Console.WriteLine("   C:\\Program Files (x86)\\Microsoft SQL Server\\140\\Tools\\Binn\\ManagementStudio\\");
                Console.WriteLine("Copy this file Microsoft.SqlServer.XEvent.Linq.dll to the folder where XELoader.exe is located and restart this program.");
                Console.ResetColor();
                return false;
            }

            return true;
        }

        static void PrintProgramDetails()
        {
            Console.WriteLine("");
            Console.WriteLine("This utility can be used to fast load contents of a set of Extended Events file to a SQL Server Database");
            Console.WriteLine("");
        }
    }
}
