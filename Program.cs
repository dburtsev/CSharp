using System;
using System.Net;
using System.Net.Sockets;
using System.Data;
using System.Data.OleDb; // dotnet add package System.Data.OleDb --version 6.0.2-mauipre.1.22054.8
using ParquetSharp; // dotnet add package ParquetSharp --version 18.1.0-beta1
using ADODB; // dotnet add package ADODB --version 7.10.3077
using Amazon; // dotnet add package AWSSDK.Core --version 3.7.13.3     3.7.6.2
using Amazon.S3; // dotnet add package AWSSDK.S3 --version 3.7.7.17
using Amazon.S3.Transfer;
using Amazon.Glue; //dotnet add package AWSSDK.Glue --version 3.7.23.14
using Amazon.Glue.Model;
using System.Data.Odbc; // dotnet add package Microsoft.Windows.Compatibility --version 8.0.4

namespace sqltoaws
{
    class Program
    {
        // The maximum network packet size for encrypted connections is 16,383 bytes.
      
        static string MSSQLConnStr = "Provider=MSOLEDBSQL;Server=...;Database=...;UID={0};PWD={1};ApplicationIntent=ReadOnly;...;PacketSize=16383;ConnectRetryCount=3;APP=" + System.Diagnostics.Process.GetCurrentProcess().ProcessName; 
        static string RedshiftConnStr = "DRIVER=Amazon Redshift (x64);"; //Server=...";
        static string bucketName = "";
        static string keyName = "";
        static string filePath = "";
        static string tblName = "";
        static string crtDestTbl = "";
        static string sqlDropDest = "";
        //static int recordset_cache = 60000; // this 'magic' number depends from your hardware/network
        private static readonly RegionEndpoint bucketRegion = RegionEndpoint.USEast1;
        private static IAmazonS3? s3Client;
        static string schemaNameSQL = "dbo";
        static string schemaNameAWS = "dbo_dev";
        static System.Data.OleDb.OleDbDataAdapter adapter = new System.Data.OleDb.OleDbDataAdapter();
        static Recordset rs = new ADODB.Recordset();
        static string ConnectionName = "";
        static string IAMRole = "";
        static string logFileName = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName + ".txt";

        static void Main(string[] args)
        {
            bool allTables = false;
            bool foundTable = false;
            bool rslt = false;
            int rowsNum = 0;
            
            string getTblsRowsCount = String.Format(@"SET NOCOUNT ON;
SELECT SCHEMA_NAME(TBL.schema_id) AS SchemaName, TBL.name AS TableName, CAST(SUM(PART.rows) AS INT) AS RowCounts
FROM sys.tables TBL
INNER JOIN sys.partitions PART ON TBL.object_id = PART.object_id
INNER JOIN sys.indexes IDX ON PART.object_id = IDX.object_id
AND PART.index_id = IDX.index_id
WHERE IDX.index_id < 2 AND TBL.name NOT IN('sysdiagrams','t_sup_prop_liens_') AND TBL.temporal_type != 1 AND TBL.name LIKE 't_%' AND SCHEMA_NAME(TBL.schema_id) = '{0}'
GROUP BY TBL.schema_id, TBL.name
ORDER BY TBL.schema_id, TBL.name DESC;", schemaNameSQL);

            Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + " Start");
            Console.WriteLine("Environment Version: {0}", Environment.Version.ToString());
            Console.WriteLine("Log file: {0}", logFileName);
            // delete log file if exist
            System.IO.File.Delete(logFileName);
            System.Data.DataTable dtTables = new System.Data.DataTable();

            // Get IP address
            IPAddress ipaddr = Dns.GetHostAddresses(Dns.GetHostName()).Where(address => address.AddressFamily == AddressFamily.InterNetwork).First();
            string ip = ipaddr.ToString();
            // Get Redshift connection name
            if (ip.StartsWith("10...")) {
                ConnectionName = "..."; // dev
            }
            else if (ip.StartsWith("10...")) {
                ConnectionName = "...."; //pre-prod
            }
            else {
                throw new Exception("Unknown IP " + ip);
            }

            // Get Role
            string jsonString = Amazon.Util.EC2InstanceMetadata.GetData("/iam/info");
            // Create a JsonNode DOM from a JSON string.
            System.Text.Json.Nodes.JsonNode infoNode = System.Text.Json.Nodes.JsonNode.Parse(jsonString)!;
            System.Text.Json.Nodes.JsonNode iamrolenode = infoNode!["InstanceProfileArn"]!;
            IAMRole = iamrolenode.ToJsonString().Trim('"').Replace("instance-profile", "role");
            // Get Redshift connection strinng  // Data Catalog
            Amazon.Glue.Model.Connection jdbcon = GetConnectionObj(ConnectionName);
            //Console.WriteLine(string.Join(Environment.NewLine,jdbcon.ConnectionProperties));
            string[] tmparr = (jdbcon.ConnectionProperties["JDBC_CONNECTION_URL"]).Split(":");
            string RedshiftServer = tmparr[2].Remove(0,2);
            //Console.WriteLine(RedshiftServer);
            string RedshiftDB = ((tmparr[3]).Split("/"))[1];
            //Console.WriteLine(RedshiftDB);
            string RedshiftUID = jdbcon.ConnectionProperties["USERNAME"];
            //Console.WriteLine(RedshiftUID);
            string RedShiftPWD = jdbcon.ConnectionProperties["PASSWORD"];
            //Console.WriteLine(RedShiftPWD);
            string RedShiftPort = ((tmparr[3]).Split("/"))[0];
            System.Data.Odbc.OdbcConnectionStringBuilder builder = new OdbcConnectionStringBuilder(RedshiftConnStr);
            builder.Add("UID", RedshiftUID);
            builder.Add("Database", RedshiftDB);
            builder.Add("Server", RedshiftServer);
            builder.Add("pwd", RedShiftPWD);
            RedshiftConnStr = builder.ConnectionString;

            ADODB.Connection objConnection = new ADODB.Connection();
            ConnectionName = "buyer_nyc_prep3_HSM";
            jdbcon = GetConnectionObj(ConnectionName);
            string UID = jdbcon.ConnectionProperties["USERNAME"];
            string PWD = jdbcon.ConnectionProperties["PASSWORD"];
            MSSQLConnStr = String.Format(MSSQLConnStr, UID, PWD);
            objConnection.ConnectionString = MSSQLConnStr;
            //Console.WriteLine(objConnection.ConnectionString );
            
            objConnection.Open();
            Console.WriteLine("Default MS SQL Server database is:" + objConnection.DefaultDatabase);
            rs.Open(getTblsRowsCount, objConnection);
            adapter.Fill(dtTables, rs);
            rs.Close();
            objConnection.Close();

            Console.WriteLine("Enter full table name like dbo.tblName or press Enter to get all dbo tables:");
            string? FullTableName = Console.ReadLine();
            // string? FullTableName = "...";

            if (FullTableName == "") { // User press Enter
                allTables = true;
                foundTable = true;
            }
            else
            {
                allTables = false;
                if (FullTableName.StartsWith("dbo.")) {
                    tblName = FullTableName.Substring(4);
                }
                else
                {
                    tblName = FullTableName;
                }
            }
            //Console.WriteLine("!allTables 1");
            if (!allTables) {
                //Console.WriteLine("!allTables 2");
                Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + " Looking for " + tblName + " in " + dtTables.Rows.Count.ToString() + " tables");
                foreach(DataRow row in dtTables.Rows) {
                    int comparison = String.Compare(row["TableName"].ToString(), tblName, comparisonType: StringComparison.OrdinalIgnoreCase);
                    // Console.WriteLine( comparison.ToString() + "  " + row["TableName"].ToString());
                    if (comparison == 0) {
                        foundTable = true;
                        rowsNum = (int)row["RowCounts"];
                        break;
                    }
                }
            }

            if (!foundTable) {
                Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + " Table " + tblName + " not found!");
                Environment.Exit(0);
            }
            
            setVariables();
            s3Client = new AmazonS3Client(bucketRegion);

            if(allTables) {
                foreach(DataRow row in dtTables.Rows) {
                    tblName = row["TableName"].ToString();
                    rowsNum = (int)row["RowCounts"];
                    Console.WriteLine(String.Format("Table {0}.{1} has {2} rows", schemaNameSQL, tblName, rowsNum));
                    if(rowsNum > 0) {
                        rslt = ProcessTable(schemaNameSQL + "." + tblName, rowsNum);
                        UploadFileAsync().Wait();
                        LoadToRedshift();
                    }
                }
            }
            else {
                // do we have schema?
                if (FullTableName.IndexOf('.') == -1 )
                    { FullTableName = String.Concat("dbo.", FullTableName); }
                Console.WriteLine(String.Format("Table {0}.{1} has {2} rows", schemaNameSQL, tblName, rowsNum)); 
                if(rowsNum > 0) {   
                    rslt = ProcessTable(FullTableName, rowsNum);
                    UploadFileAsync().Wait();
                    LoadToRedshift();
                }
            }            

            Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + " End. Press any key to close"); 
            Console.ReadKey();          
        } // static void Main
        static bool ProcessTable(string FullTableName, int rowsCount)
            {
            string selStr = "";
            
            int scale = 0;
            int precision = 0;
            string schemaName = "";
            short?[] num16Array = new Int16?[] { };
            int?[] numArray = new int?[] { };
            DateTime?[] dtArray = new DateTime?[] { };
            string?[] strArray = new string?[] { };
            long?[] num64Array = new Int64?[] { };
            bool?[] boolArray = new bool?[] { };
            char?[] charArray = new char?[] { };
            decimal?[] decArray = new decimal?[] { };
            double?[] dblArray = new double?[] { };
            float?[] singlArray = new Single?[] { };
            byte?[] byteArray = new Byte?[] { };
            Dictionary<string, int> scales = new Dictionary<string, int>();
            Dictionary<string, int> precisions = new Dictionary<string, int>();
            //if (String.IsNullOrWhiteSpace(FullTableName)) { FullTableName = "dbo.new_line"; }
            int dotPos = FullTableName.IndexOf(".", 0, FullTableName.Length, StringComparison.CurrentCulture);
            schemaName = FullTableName.Substring(0, dotPos);
            tblName = FullTableName.Substring(dotPos + 1);

            // Get the current directory.
            string curr_path = Directory.GetCurrentDirectory();
            // Console.WriteLine("The current directory is {0}", curr_path);
            filePath = Path.Combine(curr_path, (tblName + ".parquet"));
            Console.WriteLine("The output file is {0}", filePath);

            sqlDropDest = String.Format("DROP TABLE IF EXISTS {0}.{1}", schemaNameAWS, tblName);

            string getSelect = String.Format(@"SET NOCOUNT ON; 
            WITH CTE (Clm, column_id) AS
            (
            SELECT CASE
            WHEN DATA_TYPE IN('date', 'datetime', 'datetimeoffset') THEN 'CAST([' + COLUMN_NAME + '] AS DATETIME2) AS [' + COLUMN_NAME + ']'
            WHEN DATA_TYPE IN('varchar', 'nvarchar') AND (CHARACTER_MAXIMUM_LENGTH = -1 OR CHARACTER_MAXIMUM_LENGTH > 65535) THEN 'TRANSLATE(LEFT((((CAST(ISNULL([' + COLUMN_NAME + '],'''') AS VARCHAR(MAX))) COLLATE Cyrillic_General_CI_AI) COLLATE SQL_Latin1_General_Cp850_CI_AI),65535),CHAR(173) + CHAR(160) + ''§·©®¤«»¦°±'',''- S*CR """" o~'') AS [' + COLUMN_NAME + ']'
            WHEN DATA_TYPE IN('varchar', 'nvarchar') AND CHARACTER_MAXIMUM_LENGTH <> -1 THEN 'TRANSLATE(((CAST(ISNULL([' + COLUMN_NAME + '],'''') AS VARCHAR(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')) COLLATE Cyrillic_General_CI_AI) COLLATE SQL_Latin1_General_Cp850_CI_AI),CHAR(173) + CHAR(160) + ''§·©®¤«»¦°±'',''- S*CR """" o~'') AS [' + COLUMN_NAME + ']'
            WHEN DATA_TYPE IN('char', 'nchar') THEN '(CAST(ISNULL([' + COLUMN_NAME + '],'''') AS CHAR(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')) COLLATE Cyrillic_General_CI_AI) COLLATE SQL_Latin1_General_Cp850_CI_AI AS [' + COLUMN_NAME + ']'
            WHEN DATA_TYPE IN('tinyint') THEN 'CAST([' + COLUMN_NAME + '] AS SMALLINT) AS [' + COLUMN_NAME + ']'
            WHEN DATA_TYPE = 'uniqueidentifier' THEN 'CAST([' + COLUMN_NAME + '] AS CHAR(36)) AS [' + COLUMN_NAME + ']'
            WHEN DATA_TYPE IN('varbinary','binary') THEN 'CAST(''binary'' AS CHAR(6)) AS [' + COLUMN_NAME + ']'
            WHEN DATA_TYPE IN('timestamp','rowversion') THEN 'CAST([' + COLUMN_NAME + '] AS BIGINT) AS [' + COLUMN_NAME + ']'
            WHEN DATA_TYPE = 'hierarchyid' THEN 'CAST([' + COLUMN_NAME + '] AS VARCHAR(4000)) AS [' + COLUMN_NAME + ']'
            WHEN DATA_TYPE = 'xml' THEN 'TRANSLATE(LEFT((((CAST(ISNULL(CAST([' + COLUMN_NAME + '] AS NVARCHAR(MAX)),'''') AS VARCHAR(MAX))) COLLATE Cyrillic_General_CI_AI) COLLATE SQL_Latin1_General_Cp850_CI_AI),65535),CHAR(173) + CHAR(160) + ''§·©®¤«»¦°±'',''- S*CR """" o~'') AS [' + COLUMN_NAME + ']'
            ELSE '[' + COLUMN_NAME + ']' END
            , ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'
            )
            SELECT CAST('SET NOCOUNT ON; SELECT ' AS VARCHAR(MAX)), 0 AS column_id
            UNION ALL
            SELECT STRING_AGG(CAST(Clm AS VARCHAR(MAX)), ',')  WITHIN GROUP(ORDER BY column_id)  AS '---', 1
            FROM CTE
            UNION ALL
            SELECT ' FROM {0}.{1} WITH (NOLOCK);', 1025 AS column_id
            ORDER BY 2;", schemaNameSQL, tblName);
            // TRUE is converted to 1 and FALSE is converted to 0.
            // string chkIfTblExist = String.Format(@"SET NOCOUNT ON; IF (EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
            // WHERE TABLE_SCHEMA = '{0}' AND  TABLE_NAME = '{1}')) SELECT 1 ELSE SELECT 0;", schemaNameAWS, tblName);
            // SQL server and Redshift maximum precision is 38.
            // .NET decimal represents decimal numbers ranging from positive 79,228,162,514,264,337,593,543,950,335 to negative 79,228,162,514,264,337,593,543,950,335. Maximum precision is 29.
            string getDecNum = String.Format(@"SET NOCOUNT ON;
            SELECT COLUMN_NAME, CAST(NUMERIC_PRECISION AS INTEGER) AS NUMERIC_PRECISION, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE DATA_TYPE IN ('numeric', 'decimal') AND TABLE_SCHEMA = '{0}' AND  TABLE_NAME = '{1}'", schemaName, tblName);
            string getSQL = String.Format(@"SET NOCOUNT ON;
SELECT 'CREATE TABLE {0}.{2} (' + STRING_AGG(CAST(LOWER(QUOTENAME(c.COLUMN_NAME,CHAR(34))) as VARCHAR(MAX)) + ' ' +
CASE(c.DATA_TYPE)
WHEN 'bit' THEN 'BOOLEAN'
WHEN 'date' THEN 'DATE' 
WHEN 'time' THEN 'TIME' 
WHEN 'datetime' THEN 'TIMESTAMP' 
WHEN 'datetime2' THEN 'TIMESTAMP' 
WHEN 'smalldatetime' THEN 'TIMESTAMP' 
WHEN 'float' THEN 'FLOAT'
WHEN 'nchar' THEN 'CHAR(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')' 
WHEN 'char' THEN 'CHAR(' + CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')' 
WHEN 'decimal' THEN ('DECIMAL(' + CAST((COALESCE(c.NUMERIC_PRECISION, 0)) AS VARCHAR) + ',' + CAST((COALESCE(c.NUMERIC_SCALE, 0)) AS VARCHAR) + ')') 
WHEN 'numeric' THEN ('NUMERIC(' + CAST((COALESCE(c.NUMERIC_PRECISION, 0)) AS VARCHAR) + ',' + CAST((COALESCE(c.NUMERIC_SCALE, 0)) AS VARCHAR) + ')') 
/* WHEN 'decimal' THEN ('DECIMAL(29,' + CAST((COALESCE(c.NUMERIC_SCALE, 0)) AS VARCHAR) + ')') 
WHEN 'numeric' THEN ('NUMERIC(29,' + CAST((COALESCE(c.NUMERIC_SCALE, 0)) AS VARCHAR) + ')') */
WHEN 'nvarchar' THEN 'VARCHAR(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN '65535' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')' 
WHEN 'varchar' THEN 'VARCHAR(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN '65535' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')' 
WHEN 'real' THEN 'REAL' 
WHEN 'tinyint' THEN 'SMALLINT' 
WHEN 'smallint' THEN 'SMALLINT' 
WHEN 'int' THEN 'INT'
WHEN 'bigint' THEN 'INT8'
WHEN 'uniqueidentifier' THEN 'CHAR(36)' 
WHEN 'xml' THEN 'VARCHAR(65535)' 
WHEN 'varbinary' THEN 'CHAR(6)'
WHEN 'binary' THEN 'CHAR(6)'
WHEN 'timestamp' THEN 'BIGINT'
WHEN 'rowversion' THEN 'BIGINT'
WHEN 'hierarchyid' THEN 'VARCHAR(4000)'
ELSE c.DATA_TYPE END + 
CASE WHEN c.IS_NULLABLE = 'NO' THEN ' NOT NULL ' ELSE '' END 
,',') + ')'
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = '{1}' AND c.TABLE_NAME ='{2}';
            ", schemaNameAWS, schemaNameSQL,tblName);

            Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + " Start process for " + schemaNameSQL + "." + tblName);

            ADODB.Connection objConnection = new ADODB.Connection();
            objConnection.ConnectionString = MSSQLConnStr;
            objConnection.CommandTimeout = 0;
            //objConnection.
            Recordset rs = new ADODB.Recordset();
            //rs.CacheSize = ?;
            OleDbDataAdapter adapter = new System.Data.OleDb.OleDbDataAdapter();
            System.Data.DataTable dt = new System.Data.DataTable();

            objConnection.Open();

            // get CREATE TABLE 
            File.AppendAllText(logFileName, (getSQL + Environment.NewLine));
            rs.Open(getSQL, objConnection);
            rs.MoveFirst();
            crtDestTbl = rs.Fields[0].Value.ToString();
            rs.Close();

            //Console.WriteLine(getSelect);
            rs.Open(getSelect, objConnection);
            if (rs.Fields[0].Value == null) { Console.WriteLine("Empty Recordset"); }
            while (rs.EOF != true)
            {
                selStr += rs.Fields[0].Value;
                rs.MoveNext();
            }
            rs.Close();
            //Console.WriteLine(selStr);  

/*             rs.Open(getTblRowsCount, objConnection);
            if (rs.Fields[0].Value == null) { Console.WriteLine("Empty Recordset"); }
            int rowsCount = (int)rs.Fields[0].Value;
            rs.Close(); */

            rs.Open(getDecNum, objConnection);
            while (rs.EOF != true)
            {
                scales.Add(rs.Fields[0].Value.ToString(), (int)rs.Fields[2].Value);
                precisions.Add(rs.Fields[0].Value.ToString(), (int)rs.Fields[1].Value);
                rs.MoveNext();
            }            
            rs.Close();

            File.AppendAllText(logFileName, (selStr + Environment.NewLine));

            rs.Open(selStr, objConnection, ADODB.CursorTypeEnum.adOpenForwardOnly, ADODB.LockTypeEnum.adLockReadOnly);
            // if (rs.CacheSize < recordset_cache) { rs.CacheSize = (int)rowsCount; }
            // else { rs.CacheSize = recordset_cache; }
            // if (rowsCount > recordset_cache) { rs.CacheSize = recordset_cache; }
            // else { rs.CacheSize = rowsCount; }
            rs.CacheSize = rowsCount;
            dt.MinimumCapacity = rowsCount;
 
            DateTime startDate = DateTime.Now;
            for(int i = 0; i < 4; i++)
            {
                try
                {
                    dt.BeginLoadData();
                    adapter.Fill(dt, rs);
                }
                catch (Exception e)
                {
                    File.AppendAllText(logFileName, ("Error in " + tblName + Environment.NewLine));
                    File.AppendAllText(logFileName, ($"{e}"));
                    Thread.Sleep(1000);
                    if (i < 3)
                    {
                    if (objConnection.State != 1) { objConnection.Open(); } // 1 = Open
                        continue;
                    }
                    else
                    {
                        throw;
                    }
                }
                finally { dt.EndLoadData(); }
            }


            //Console.WriteLine("MinimumCapacity is {0}, CacheSize is {1}",dt.MinimumCapacity,rs.CacheSize);

            TimeSpan diff = DateTime.Now.Subtract(startDate);
            Console.WriteLine(String.Format(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + " Load DataTable in {0}:{1}:{2} and close connection to database", diff.Hours,diff.Minutes,diff.Seconds));

            rs.Close();
            rs = null;
            objConnection.Close();
            objConnection = null;

            //GC.Collect();
            //GC.WaitForPendingFinalizers();
            //Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + " done with garbage collector");
            startDate = DateTime.Now;

            ParquetSharp.Column[] columns = new ParquetSharp.Column[dt.Columns.Count];

            for (int i = 0; i < dt.Columns.Count; i++)
            {
                //Console.WriteLine(" column {0} datatype {1} typecode {2}", dt.Columns[i].ColumnName, dt.Columns[i].DataType, Type.GetTypeCode(dt.Columns[i].DataType));
                switch (System.Type.GetTypeCode(dt.Columns[i].DataType))
                {
                    case TypeCode.Int16: columns[i] = new ParquetSharp.Column<Int16?>(dt.Columns[i].ColumnName); break;
                    case TypeCode.Int32: columns[i] = new ParquetSharp.Column<int?>(dt.Columns[i].ColumnName); break;
                    case TypeCode.Int64: columns[i] = new ParquetSharp.Column<Int64?>(dt.Columns[i].ColumnName); break;
                    case TypeCode.DateTime: columns[i] = new ParquetSharp.Column<DateTime?>(dt.Columns[i].ColumnName); break;
                    case TypeCode.String: columns[i] = new ParquetSharp.Column<string?>(dt.Columns[i].ColumnName); break;
                    case TypeCode.Boolean: columns[i] = new ParquetSharp.Column<bool?>(dt.Columns[i].ColumnName); break;
                    case TypeCode.Decimal:
                        {   
                            scale = scales[dt.Columns[i].ColumnName];
                            precision = precisions[dt.Columns[i].ColumnName];                          
                            columns[i] = new ParquetSharp.Column<decimal?>(dt.Columns[i].ColumnName, LogicalType.Decimal(precision, scale)); break;
                        }
                    case TypeCode.Single: columns[i] = new ParquetSharp.Column<Single?>(dt.Columns[i].ColumnName); break;
                    case TypeCode.Double: columns[i] = new ParquetSharp.Column<double?>(dt.Columns[i].ColumnName); break;
                    case TypeCode.Byte: columns[i] = new ParquetSharp.Column<Byte?>(dt.Columns[i].ColumnName); break;
                    //case TypeCode.Object: columns[i] = new ParquetSharp.Column<ByteArray>(dt.Columns[i].ColumnName); break;
                    default: Console.WriteLine("Missing column {0}, type {1}", dt.Columns[i], dt.Columns[i].DataType.Name); break;
                }
            }

            using ParquetFileWriter file = new ParquetFileWriter(filePath, columns);
            using RowGroupWriter rowGroup = file.AppendRowGroup();
            EnumerableRowCollection<DataRow> enumdt = dt.AsEnumerable();

            for (int i = 0; i < dt.Columns.Count; i++)
            {
                switch (System.Type.GetTypeCode(dt.Columns[i].DataType))
                {
                    case TypeCode.Int16:
                        {
                            num16Array = enumdt.Select(d => d.Field<Int16?>(dt.Columns[i].ColumnName)).ToArray();
                            using (LogicalColumnWriter<short?> int16Writer = rowGroup.NextColumn().LogicalWriter<Int16?>())
                            {
                                int16Writer.WriteBatch(num16Array);
                            }
                            Array.Clear(num16Array, 0, num16Array.Length);
                            break;
                        }
                    case TypeCode.Int32:
                        {
                            numArray = enumdt.Select(d => d.Field<int?>(dt.Columns[i].ColumnName)).ToArray();
                            using (LogicalColumnWriter<int?> intWriter = rowGroup.NextColumn().LogicalWriter<int?>())
                            {
                                intWriter.WriteBatch(numArray);
                            }
                            Array.Clear(numArray, 0, numArray.Length);
                            break;
                        }
                    case TypeCode.Int64:
                        {
                            num64Array = enumdt.Select(d => d.Field<Int64?>(dt.Columns[i].ColumnName)).ToArray();
                            using (LogicalColumnWriter<long?> int64Writer = rowGroup.NextColumn().LogicalWriter<Int64?>())
                            {
                                int64Writer.WriteBatch(num64Array);
                            }
                            Array.Clear(num64Array, 0, num64Array.Length);
                            break;
                        }
                    case TypeCode.DateTime:
                        {

                            dtArray = enumdt.Select(d => d.Field<DateTime?>(dt.Columns[i].ColumnName)).ToArray();
                            using (LogicalColumnWriter<DateTime?> timestampWriter = rowGroup.NextColumn().LogicalWriter<DateTime?>())
                            {
                            timestampWriter.WriteBatch(dtArray);
                            }
                            Array.Clear(dtArray, 0, dtArray.Length);
                            break;
                        }    
                    case TypeCode.String:
                        {
                            strArray = enumdt.Select(d => d.Field<string?>(dt.Columns[i].ColumnName)).ToArray();
                            using (LogicalColumnWriter<string?> strWriter = rowGroup.NextColumn().LogicalWriter<string?>())
                            {
                                strWriter.WriteBatch(strArray);
                            }
                            Array.Clear(strArray, 0, strArray.Length);
                            break;
                        }
                    case TypeCode.Boolean:
                        {
                            boolArray = enumdt.Select(d => d.Field<bool?>(dt.Columns[i].ColumnName)).ToArray();
                            using (LogicalColumnWriter<bool?> boolWriter = rowGroup.NextColumn().LogicalWriter<bool?>())
                            {
                                boolWriter.WriteBatch(boolArray);
                            }
                            Array.Clear(boolArray, 0, boolArray.Length);
                            break;
                        }
                    case TypeCode.Decimal:
                        {
                            decArray = enumdt.Select(d => d.Field<decimal?>(dt.Columns[i].ColumnName)).ToArray();
                            using (LogicalColumnWriter<decimal?> decWriter = rowGroup.NextColumn().LogicalWriter<decimal?>())
                            {
                                decWriter.WriteBatch(decArray);
                            }
                            Array.Clear(decArray, 0, decArray.Length);
                            break;
                        }
                    case TypeCode.Single:
                        {
                            singlArray = enumdt.Select(d => d.Field<Single?>(dt.Columns[i].ColumnName)).ToArray();
                            using (LogicalColumnWriter<float?> singlWriter = rowGroup.NextColumn().LogicalWriter<Single?>())
                            {
                                singlWriter.WriteBatch(singlArray);
                            }
                            Array.Clear(singlArray, 0, singlArray.Length);
                            break;
                        }
                    case TypeCode.Double:   
                        {
                            dblArray = enumdt.Select(d => d.Field<double?>(dt.Columns[i].ColumnName)).ToArray();
                            using (LogicalColumnWriter<double?> singlWriter = rowGroup.NextColumn().LogicalWriter<double?>())
                            {
                                singlWriter.WriteBatch(dblArray);
                            }
                            Array.Clear(dblArray, 0, dblArray.Length);
                            break;
                        }     
                    case TypeCode.Byte:   
                        {
                            byteArray = enumdt.Select(d => d.Field<Byte?>(dt.Columns[i].ColumnName)).ToArray();
                            using (LogicalColumnWriter<byte?> singlWriter = rowGroup.NextColumn().LogicalWriter<Byte?>())
                            {
                                singlWriter.WriteBatch(byteArray);
                            }
                            Array.Clear(byteArray, 0, byteArray.Length);
                            break;
                        }                                        
                }
            }  //for  
            file.Close();
            diff = DateTime.Now.Subtract(startDate);
            Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + String.Format(" DataTable Columns to Parquet {3} in {0}:{1}:{2}", diff.Hours,diff.Minutes,diff.Seconds,filePath));
            return true;
            }  // static bool ProcessTable
        private static void setVariables()
        {
            // we use Redshift table to store variables
            // CREATE TABLE ods.etl_var_tbl(
	        // var_nm VARCHAR(100) NOT NULL,
	        // var_val VARCHAR(256)) DISTSTYLE EVEN;
            OdbcCommand get_var_value = new System.Data.Odbc.OdbcCommand("SELECT var_val FROM ods.etl_var_tbl WHERE var_nm = ? ;");
            OdbcParameter param = new System.Data.Odbc.OdbcParameter("var_nm", System.Data.Odbc.OdbcType.VarChar, 256, "var_nm");
            param.Direction = System.Data.ParameterDirection.Input;
            get_var_value.Parameters.Add(param);

            using (OdbcConnection connDest = new OdbcConnection(RedshiftConnStr))
            {
                connDest.Open();
                get_var_value.Connection = connDest;
                get_var_value.Parameters["var_nm"].Value = "bucket";
                bucketName = (string)get_var_value.ExecuteScalar();                
            }
        }
        private static async Task<Amazon.Glue.Model.Connection> GetConnectionObjasync(string ConnName)
        {
            AmazonGlueClient client = new AmazonGlueClient();
            GetConnectionRequest request = new GetConnectionRequest() { Name = ConnName };
            GetConnectionResponse response = await client.GetConnectionAsync(request);
            return response.Connection;
        }
        private static Amazon.Glue.Model.Connection GetConnectionObj(string ConnName)
        {
            Task<Amazon.Glue.Model.Connection> task = GetConnectionObjasync(ConnName);
            task.Wait();
            return task.Result;            
        }
        private static async Task UploadFileAsync()
        {
            try
            {
                TransferUtility fileTransferUtility =
                    new TransferUtility(s3Client);

                // Option 1. Upload a file. The file name is used as the object key name.
                // await fileTransferUtility.UploadAsync(filePath, bucketName);
                // Console.WriteLine("Upload 1 completed");

                // Option 2. Specify object key name explicitly.
                keyName = @"SQLTOAWS/" + Path.GetFileName(filePath);
                await fileTransferUtility.UploadAsync(filePath, bucketName, keyName);
                Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + " Upload to s3 {0} is completed", keyName);

                // Option 3. Upload data from a type of System.IO.Stream.
                // using (var fileToUpload = 
                //     new FileStream(filePath, FileMode.Open, FileAccess.Read))
                // {
                //     await fileTransferUtility.UploadAsync(fileToUpload,
                //                                bucketName, keyName);
                // }
                // Console.WriteLine("Upload 3 completed");

                // // Option 4. Specify advanced settings.
                // var fileTransferUtilityRequest = new TransferUtilityUploadRequest
                // {
                //     BucketName = bucketName,
                //     FilePath = filePath,
                //     StorageClass = S3StorageClass.StandardInfrequentAccess,
                //     PartSize = 6291456, // 6 MB.
                //     Key = keyName,
                //     CannedACL = S3CannedACL.PublicRead
                // };
                // fileTransferUtilityRequest.Metadata.Add("param1", "Value1");
                // fileTransferUtilityRequest.Metadata.Add("param2", "Value2");

                // await fileTransferUtility.UploadAsync(fileTransferUtilityRequest);
                // Console.WriteLine("Upload 4 completed");
            }
            catch (AmazonS3Exception e)
            {
                Console.WriteLine("Error encountered on server. Message:'{0}' when writing an object", e.Message);
                throw;
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
                throw;
            }
        File.Delete(filePath);
        } // Task UploadFileAsync()
        static bool LoadToRedshift()
        {
            Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + " Start Redshift COPY command");
            
            using (OdbcConnection connDest = new OdbcConnection(RedshiftConnStr))
            {
                connDest.Open();
                OdbcCommand commdest = new System.Data.Odbc.OdbcCommand(sqlDropDest, connDest);
                commdest.ExecuteNonQuery(); // drop table
                commdest.CommandText = crtDestTbl;
                commdest.ExecuteNonQuery(); // create table
                string copyCommand = String.Format(@"COPY {4}.{0} FROM 's3://{1}/{2}' IAM_ROLE '{3}' PARQUET ", tblName, bucketName, keyName, IAMRole, schemaNameAWS);
                commdest.CommandText = copyCommand;
                Console.WriteLine(copyCommand);
                commdest.ExecuteNonQuery();
                commdest.CommandText = "SELECT CAST(PG_LAST_COPY_COUNT() AS TEXT) AS pg_last_copy_count";
                string rowsNumsStr = (commdest.ExecuteScalar()).ToString();
                Console.WriteLine(DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + " COPY command inserted {0} rows", rowsNumsStr);
            } // OdbcConnection
            AmazonS3Client client = new AmazonS3Client();
            client.DeleteObjectAsync(bucketName,keyName);
            //Console.WriteLine("Finished Redshift COPY command");
            return true;
        }
    } // class Program
} // namespace sqltoaws
