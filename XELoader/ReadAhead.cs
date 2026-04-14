using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Microsoft.Data.SqlClient;
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
    }
}
