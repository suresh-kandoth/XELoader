# XELoader

Utility to fast load contents of a set of SQL Server Extended Events files (.xel) to a SQL Server Database.

**Runtime: .NET 9**

## Required Parameters

Use `-D` or `-f` for input files to feed.
Utility expects file names to be in their original format: `SessionName_PartitionID_TimeStampInfo.xel`

| Parameter | Description |
|-----------|-------------|
| `-D` | Specify path to a folder of `.xel` files that needs to be processed. Do not specify the trailing slash for the folder; keep the files local to the machine for fast performance. If this folder contains `.xel` files from multiple XE sessions, use `-p` parameter to specify file pattern to process. |
| `-f` | Specify path to a single `.xel` file that needs to be processed. If there are multiple files from the same XE session then use the `-D` parameter. |
| `-S` | Specify the server name and instance name to which you want to save the XE data [default: `(local)`]. For fast performance use TCP by specifying the server name in the format: `-SMachineName,TcpPortNumber` |

## Optional Parameters

| Parameter | Description |
|-----------|-------------|
| `-d` | Specify the database name to use for saving the processed XE data [default: `XE_Import`]. If the database name exists, it will be used instead of creating a new one. You can pre-create the database on a specific location, multiple files and other filegroup options for better performance. |
| `-U` | SQL Server login name. When specified, SQL Server authentication is used instead of Windows Integrated authentication. |
| `-P` | SQL Server login password. Use together with `-U` for SQL Server authentication. |
| `-w` | Use this parameter to indicate if the existing database needs to be dropped and recreated from scratch. You do not specify any value for parameter. |
| `-c` | Use this parameter to indicate if all existing tables for this collection of events needs to be cleared and data reloaded. If this option is not passed and the table already exists then new data will be appended to existing tables. |
| `-a` | Use this parameter to append data to the existing database and tables. You do not specify any value for parameter. |
| `-v` | Specify version of the logs being collected. |
| `-I` | Specify the Index type used to store the data in the database — `RowStore` or `ColumnStore` [default: `ColumnStore`]. ColumnStore will provide great compression for repeat data and performance while doing aggregate queries. ColumnStore does not support large data types so we have to truncate long strings and xml columns — see parameters `-L` and `-X`. RowStore will save all the data in the XE files — so this can use up lot of storage and also slow while querying. |
| `-s` | Specify the schema under which to create the objects [default: `xel`]. For each event captured in the XE session a table will be created under this schema. |
| `-b` | Specify the bulk copy batch size in multiples of 100K [default: `1048576`]. When using ColumnStore leave the batch size to the default of 1 million since it is the ideal rowgroup size. |
| `-t` | Specify the number of threads to use for processing [default: # of processors in the system capped at 16]. Each XEL file will be processed independently on a separate thread. |
| `-p` | Specify the name pattern of files that needs to be processed from a folder of `.xel` files [default: `*.xel`]. Supports wildcard patterns using `*` (match any characters) and `?` (match single character). Examples: `-p"SessionName*.xel"`, `-p"*AlwaysOn*.xel"`, `-p"Session?_*.xel"` |
| `-R` | Specify whether to perform read-ahead (`y`) or not (`n`) [default: `y`]. Read ahead can populate the file system cache and potentially improve event processing performance depending on system memory. |
| `-z` | Specify what timezone you want to use when processing time information to convert UTC time to local time. Every event table will have three time columns: (1) UTC time in smalldatetime, (2) UTC time in datetime2, (3) LOCAL time in datetime2. If you do not pass any value for this parameter, the local timezone of the importing system is used. |
| `-l` | Use this parameter to indicate if you want to disable use of LOB with ColumnStore index in newer versions. SQL Server 2017 and above allow LOB columns on ColumnStore index. You do not specify any value for parameter. |
| `-L` | Specify truncation length for large strings when using ColumnStore index [default: `1024`]. |
| `-X` | Specify truncation length for large XML values when using ColumnStore index [default: `4000`]. |
| `-x` | Use this parameter to indicate if you store xml values in `varchar(max)` columns instead of xml columns. Preferred for xml values with more than 128 depth level. You do not specify any value for parameter. |
| `-B` | Specify truncation length for large binary values when using ColumnStore index [default: `1024`]. |
| `-e` | Specify number of errors or exceptions you are willing to tolerate [default: `100`]. Remember that skipping errors while loading the events can affect the accuracy of the data imported. |
| `-h` | Specify how many bytes represent one frame in callstack action [default: `8`]. Use 8 for 64-bit servers, 4 for 32-bit servers. |
| `-j` | Specify what debugger command you want to execute for each frame in callstack action [default: `ln`]. |
| `-T` | Trust the server's SSL certificate [default: do not trust]. Use this when connecting to a server with a self-signed certificate. You do not specify any value for parameter. |
| `-E` | Disable encryption for the SQL connection [default: encryption enabled]. Use this when encryption is not required or causing connection issues. You do not specify any value for parameter. |
| `-V` | Enable verbose output including full exception stack traces [default: off]. Use this when troubleshooting errors to get detailed diagnostic information. You do not specify any value for parameter. |

## Assembly Dependencies

- `Microsoft.SqlServer.XEvent.Linq.dll` (bundled in `XEventSDK\2025\`)
- `Microsoft.Data.SqlClient` (NuGet package)

## Authentication Modes

- **Windows Integrated** (default): Uses the current Windows credentials.
- **SQL Server Authentication**: Use `-U` and `-P` parameters to specify SQL Server login credentials.

## License

MIT License — see [LICENSE](LICENSE) for details.
