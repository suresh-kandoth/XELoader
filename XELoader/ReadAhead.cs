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
    public class ReadAhead
    {
        private String m_File_To_Read;
        private int m_ReadAheadChunk;

        public ReadAhead(String in_File_To_Read)
        {
            m_File_To_Read = in_File_To_Read;
            m_ReadAheadChunk = 4 * (1024 * 1024);
        }

        public void DoReadAhead()
        {
            try
            {
                using (FileStream s = new FileStream(m_File_To_Read, FileMode.Open, FileAccess.Read, FileShare.Read, m_ReadAheadChunk, FileOptions.SequentialScan))
                {
                    byte[] DataBuffer = new byte[m_ReadAheadChunk];

                    int Loops = 0;
                    int BytesRead = s.Read(DataBuffer, 0, m_ReadAheadChunk);
                    while (BytesRead > 0)
                    {
                        BytesRead = 0;
                        Loops++;
                        if (0 == Loops % 10)
                        {
                            Thread.Sleep(1);
                        }
                        BytesRead = s.Read(DataBuffer, 0, m_ReadAheadChunk);
                    }
                }
            }
            catch (Exception e)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Thread {0} : ***  Exception encountered in DoReadAhead routine *** ", Thread.CurrentThread.ManagedThreadId);
                Console.WriteLine("Thread {0} : Exception message : {1}  ", Thread.CurrentThread.ManagedThreadId, e.Message);
                Console.WriteLine("Thread {0} : Exception code : {1}  ", Thread.CurrentThread.ManagedThreadId, System.Runtime.InteropServices.Marshal.GetHRForException(e));  //Using e.HResult gives CS0122 in new versions of .NET
                // Console.WriteLine("Thread {0} : Exception stack : {1}  ", Thread.CurrentThread.ManagedThreadId, e.StackTrace);
                Console.ResetColor();

            }
        }
    }
}
