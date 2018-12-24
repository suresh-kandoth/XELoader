# XELoader

This utility can be used to fast load contents of a set of Extended Events file to a SQL Server Database

*** Parameter Details ***
*** Parameters are case sensitive, no spaces allowed after parameter name and its value ***

*** Required Parameters ***
    Use -D or -f for input files to feed
    Utility expects file names to be in thier original format: SessionName_PartitionID_TimeStampInfo.xel

-D : specify path to a folder of .xel files that needs to be processed
      do not specify the trailing slash for the folder, keep the files local to the machine for fast performance
      if this folder contains .xel files from multiple XE session, use -p parameter to specify file pattern to process
-f : specify path to a single .xel file that needs to be processed
      if there are multiple files from the same XE session then use the -D parameter
-S : specify the server name and instance name to which you want to save the XE data [default : (local) ]
      for fast performance use TCP by specifying the server name in the format : -SMachineName,TcpPortNumber

*** Optional parameters ***

-d : specify the database name to use for saving the processed XE data [default : XE_Import]
      if the database name exists, it will be used instead of creating a new one
      you can pre-create the database on a specific location, multiple files and other filegroup options for better performance
-w : use this parameter to indicate if the existing database needs to be (wiped) - dropped and recreated from scratch
      you do not specify any value for parameter
-c : use this parameter to indicate if all existing tables for this collection of events needs to be cleared and data reloaded
      if this option is not passed and the table already exists then new data will be appended to existing tables
      you do not specify any value for parameter
-a : use this parameter to indicate if data from events needs to be appended to existing tables in the database provided
      use this option for incremental loads into an existing database that already has imported information
      when this option is specified no metadata work happens [database level and table level]
      you do not specify any value for parameter
-s : specify the schema under which to create the objects [default : xel]
      for each event captured in the XE session a table will be created under this schema
-I : specify the Index type used to store the data in the database - RowStore or ColumnStore [default : ColumnStore]
      ColumnStore will provide great compression for repeat data and performance while doing aggregate queries
      ColumnStore does not support large data types so we have to truncate long strings and xml columns - see parameters -L and -X
      RowStore will save all the data in the XE files - so this can use up lot of storage and also slow while querying
-b : specify the bulk copy batch size in multiples of 100K [default : 1048576]
      when using ColumnStore leave the batch size to the default of 1 million since it is the ideal rowgroup size
-t : specify the number of threads to use for processing [default : # of processors in the system capped at 8]
      each XEL file will be processed independently on a seperate thread, the more parallelism the faster you can load the files
      If you need more than the default 8 threads to process the XEL files in parallel, then provide a value higher than 8
      If you need to limit the number of threads processing the XEL files, then provide a value like 1 or 2 for this parameter
-p : specify the name pattern of files that needs to be processed from a folder of .xel files [default : "*.xel"]
      if there are XE files from multiple XE sessions use this parameter to selectively process files of interest
      Example to use: -p"activity_tracing*.xel" will process all XEL files that start with activity_tracing in that folder
-R : specify whether to perform read-ahead (y) or not (n) [default : y]
      read ahead can populate the file system cache and potentially improve event processing performance depending on system memory
-z : specify what timezone you want to use when processing time information to convert utc time to local time
     every event table will have three time columns representing the time of the event
      1.UTC time in smalldatetime format used for graphing and other visuals
      2.UTC time in datetime2 format used for accurate event sequence down to the 100 nanoseconds unit
      3.LOCAL time in datetime2 format used for accurate event sequence down to the 100 nanoseconds unit
     If you do not pass any value for this parameter, the local timezone of the importing system is used
-l : use this parameter to indicate if you want to disable use of LOB with ColumnStore index in newer versions
      SQL Server 2017 and above allow LOB columns on ColumnStore index
      you do not specify any value for parameter
-L : specify truncation length for large strings when using ColumnStore index [default : 1024]
-X : specify truncation length for large XML values when using ColumnStore index [default : 4000]
-x : use this parameter to indicate if you store xml values in varchar(max) columns instead of xml columns
      preferred for xml values with more than 128 depth level
      you do not specify any value for parameter
-B : specify truncation length for large binary values when using ColumnStore index [default : 1024]
-e : specify number of errors or exceptions you are willing to tolerate [default : 100]
      remember that skipping errors while loading the events can affect the accuracy of the data imported
-h : specify how many bytes represent one frame in callstack action [default : 8]
      if you collected the extended events on a 64-bit server then you need 8 bytes for a frame
      if you collected the extended events on a 32-bit server then you need 4 bytes for a frame
-j : specify what debugger command you want to execute for each frame in callstack action [default : ln]

*** End of parameters ***

Assembly dependencies:

Microsoft.SqlServer.XEvent.Linq.dll [version 14.x.x.x]
Microsoft.SqlServer.XE.Core.dll [version 14.x.x.x]


