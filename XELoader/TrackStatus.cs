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
    public class TrackStatus
    {
        private ReaderWriterLockSlim create_table_Lock = new ReaderWriterLockSlim();
        private ReaderWriterLockSlim metadata_Lock = new ReaderWriterLockSlim();
        private ReaderWriterLockSlim error_truncation_Lock = new ReaderWriterLockSlim();
        public bool IsTableCreated = false;
        public String m_filePatternInUse;
        public DateTime m_Start_Time_To_Process;
        public int m_Number_of_Files;
        public Int64 m_Number_of_Events;
        public int m_Number_of_Tables;
        public int m_Number_of_Errors;
        public int m_Number_of_StringTruncations;
        public int m_Number_of_XMLTruncations;
        public int m_Number_of_BinaryTruncations;

        public TrackStatus()
        {
            m_Start_Time_To_Process = DateTime.Now;
            m_filePatternInUse = "";
            m_Number_of_Files = 0;
            m_Number_of_Events = 0;
            m_Number_of_Tables = 0;
            m_Number_of_Errors = 0;
            m_Number_of_StringTruncations = 0;
            m_Number_of_XMLTruncations = 0;
            m_Number_of_BinaryTruncations = 0;
        }

        public void PrintStatistics()
        {
            DateTime end_Time = DateTime.Now;
            Console.WriteLine("");
            Console.ForegroundColor = ConsoleColor.Green;

            Console.WriteLine("*** Statistics for Import ***");
            Console.WriteLine(" Files imported with leading pattern : {0}", m_filePatternInUse);
            Console.WriteLine(" Time taken to process all files     : {0}", end_Time.Subtract(m_Start_Time_To_Process));
            Console.WriteLine(" Total number of files processed     : {0}", m_Number_of_Files);
            Console.WriteLine(" Total number of events processed    : {0}", m_Number_of_Events);
            Console.WriteLine(" Total number of tables processed    : {0}", m_Number_of_Tables);
            Console.WriteLine(" Total number of errors encountered  : {0}", m_Number_of_Errors);
            Console.WriteLine(" Total Strings truncated             : {0}", m_Number_of_StringTruncations);
            Console.WriteLine(" Total XML truncated                 : {0}", m_Number_of_XMLTruncations);
            Console.WriteLine(" Total Binary truncated              : {0}", m_Number_of_BinaryTruncations);

            Console.ResetColor();
        }

        public void acquire_create_table_Lock(bool WriteMode)
        {
            if (true == WriteMode)
                create_table_Lock.EnterWriteLock();
            else
                create_table_Lock.EnterReadLock();
        }

        public void release_create_table_Lock(bool WriteMode)
        {
            if (true == WriteMode)
                create_table_Lock.ExitWriteLock();
            else
                create_table_Lock.ExitReadLock();
        }

        public void acquire_metadata_Lock(bool WriteMode)
        {
            if (true == WriteMode)
                metadata_Lock.EnterWriteLock();
            else
                metadata_Lock.EnterReadLock();
        }

        public void release_metadata_Lock(bool WriteMode)
        {
            if (true == WriteMode)
                metadata_Lock.ExitWriteLock();
            else
                metadata_Lock.ExitReadLock();
        }

        public void acquire_error_truncation_Lock(bool WriteMode)
        {
            if (true == WriteMode)
                error_truncation_Lock.EnterWriteLock();
            else
                error_truncation_Lock.EnterReadLock();
        }

        public void release_error_truncation_Lock(bool WriteMode)
        {
            if (true == WriteMode)
                error_truncation_Lock.ExitWriteLock();
            else
                error_truncation_Lock.ExitReadLock();
        }


    }
}
