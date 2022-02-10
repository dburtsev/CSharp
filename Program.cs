using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.OleDb; // dotnet add package System.Data.OleDb --version 6.0.2-mauipre.1.22054.8
using System.IO;
using ParquetSharp; // dotnet add package ParquetSharp
using ADODB; // dotnet add package ADODB --version 7.10.3077
using Amazon; // dotnet add package AWSSDK.Core --version 3.7.6.2
using Amazon.S3; // dotnet add package AWSSDK.S3 --version 3.7.7.17
using Amazon.S3.Transfer;
using System.Data.Odbc; // dotnet add package Microsoft.Windows.Compatibility --version 6.0.2-mauipre.1.22054.8

// launch.json "console": "integratedTerminal"

namespace sqltoaws
{
    class Program
    {
        static string MSSQLConnStr = "Provider=MSOLEDBSQL;Database=xyz;Trusted_Connection=yes;";
        static string RedshiftConnStr = "DRIVER=Amazon Redshift (x64);Server=xyz.redshift.amazonaws.com;Database=xyz;UID=xyz;pwd=xyz;Port=5439";
        static string bucketName = "";
        static string keyName = "";
        static string filePath = "";
        static string tblName = "";
        static string crtDestTbl = "";
        static string sqlDropDest = "";
        static string accesskeyDest = "";
        static string secretkeyDest = "";
        static int recordset_cache = 60000; // this 'magic' number depends from your hardware/network
        private static readonly RegionEndpoint bucketRegion = RegionEndpoint.USEast1;
        private static IAmazonS3? s3Client;
        const int precision = 29; // .NET limitation

        static void Main(string[] args)
        {
            string? FullTableName;
            Console.WriteLine("Version: {0}", Environment.Version.ToString());
            Console.WriteLine("Enter full table name like dbo.tblName:");
            FullTableName = Console.ReadLine();

            // do we have schema?
            if (FullTableName.IndexOf('.') == -1 )
            { FullTableName = String.Concat("dbo.", FullTableName); }

            
            setVariables();
            s3Client = new AmazonS3Client(bucketRegion);

            bool rslt = ProcessTable(FullTableName);
            UploadFileAsync().Wait();
            LoadToRedshift();

            Console.WriteLine("Press any key to close"); 
            Console.ReadKey();          
        } // static void Main
        static bool ProcessTable(string? FullTableName)
            {
            string selStr = "";
            
            int scale = 0;			
            string schemaName = "";
            var num16Array = new Int16?[] { };
            var numArray = new int?[] { };
            var dtArray = new DateTime?[] { };
            var strArray = new string?[] { };
            var num64Array = new Int64?[] { };
            var boolArray = new bool?[] { };
            var charArray = new char?[] { };
            var decArray = new decimal?[] { };
            var dblArray = new double?[] { };
            var singlArray = new Single?[] { };
            var byteArray = new Byte?[] { };
            Dictionary<string, int> scales = new Dictionary<string, int>();
            //if (String.IsNullOrWhiteSpace(FullTableName)) { FullTableName = "dbo.new_line"; }
            int dotPos = FullTableName.IndexOf(".", 0, FullTableName.Length, StringComparison.CurrentCulture);
            schemaName = FullTableName.Substring(0, dotPos);
            tblName = FullTableName.Substring(dotPos + 1);

            // Get the current directory.
            string curr_path = Directory.GetCurrentDirectory();
            //Console.WriteLine("The current directory is {0}", curr_path);
            filePath = Path.Combine(curr_path, (tblName + ".parquet"));
            Console.WriteLine("The output file is {0}", filePath);

            sqlDropDest = String.Format("DROP TABLE IF EXISTS stage.{0}", tblName);

            string getSelect = String.Format(@"SET NOCOUNT ON; 
            WITH CTE (Clm, column_id) AS
            (
            SELECT CASE
            WHEN DATA_TYPE IN('date', 'datetime', 'datetimeoffset') THEN 'CAST(' + COLUMN_NAME + ' AS DATETIME2) AS ' + COLUMN_NAME
            WHEN DATA_TYPE IN('varchar', 'nvarchar', 'xml') AND (CHARACTER_MAXIMUM_LENGTH = -1 OR CHARACTER_MAXIMUM_LENGTH > 65535) THEN '((CAST(ISNULL(' + COLUMN_NAME + ','''') AS VARCHAR(65535))) COLLATE Cyrillic_General_CI_AI) COLLATE SQL_Latin1_General_CP1_CI_AI) AS ' + COLUMN_NAME
            WHEN DATA_TYPE IN('varchar', 'nvarchar') AND CHARACTER_MAXIMUM_LENGTH <> -1 THEN '((CAST(ISNULL(' + COLUMN_NAME + ','''') AS VARCHAR(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + ')) COLLATE Cyrillic_General_CI_AI) COLLATE SQL_Latin1_General_CP1_CI_AI) AS ' + COLUMN_NAME
            WHEN DATA_TYPE IN('char', 'nchar') THEN '(CAST(ISNULL(' + COLUMN_NAME + ','''') AS CHAR(' + CAST(CHARACTER_MAXIMUM_LENGTH AS VARCHAR) + '))  COLLATE Cyrillic_General_CI_AI) COLLATE SQL_Latin1_General_CP1_CI_AI AS ' + COLUMN_NAME
            WHEN DATA_TYPE IN('tinyint') THEN 'CAST(' + COLUMN_NAME + ' AS SMALLINT) AS ' + COLUMN_NAME
            ELSE COLUMN_NAME END
            , ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{0}' AND TABLE_NAME = '{1}'
            )
            SELECT CAST('SET NOCOUNT ON; SELECT ' AS VARCHAR(MAX)), 0 AS column_id
            UNION ALL
            SELECT STRING_AGG(CAST(Clm AS VARCHAR(MAX)), ',')  WITHIN GROUP(ORDER BY column_id)  AS '---', 1
            FROM CTE
            UNION ALL
            SELECT ' FROM {0}.{1};', 1025 AS column_id
            ORDER BY 2;", schemaName, tblName);
            string getTblRowsCount = String.Format(@"SET NOCOUNT ON;
            SELECT CAST(p.rows AS INT) AS RowCounts
            FROM sys.tables t 
            INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
            INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = '{0}' AND t.name = '{1}' 
            GROUP BY t.name, s.name, p.rows;", schemaName, tblName);
            // TRUE is converted to 1 and FALSE is converted to 0.
            string chkIfTblExist = String.Format(@"SET NOCOUNT ON; IF (EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{0}' AND  TABLE_NAME = '{1}')) SELECT 1 ELSE SELECT 0;", schemaName, tblName);
            // SQL server and Redshift maximum precision is 38.
            // .NET decimal represents decimal numbers ranging from positive 79,228,162,514,264,337,593,543,950,335 to negative 79,228,162,514,264,337,593,543,950,335. Maximum precision is 29.
            string getDecNum = String.Format(@"SET NOCOUNT ON;
            SELECT COLUMN_NAME, NUMERIC_SCALE FROM INFORMATION_SCHEMA.COLUMNS WHERE DATA_TYPE IN ('numeric', 'decimal') AND TABLE_SCHEMA = '{0}' AND  TABLE_NAME = '{1}'", schemaName, tblName);
            string getSQL = String.Format(@"SET NOCOUNT ON;
SELECT 'CREATE TABLE stage.{1} (' + STRING_AGG(CAST(LOWER(c.COLUMN_NAME) as VARCHAR(MAX)) + ' ' +
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
/* WHEN 'decimal' THEN ('DECIMAL(' + CAST((COALESCE(c.NUMERIC_PRECISION, 0)) AS VARCHAR) + ',' + CAST((COALESCE(c.NUMERIC_SCALE, 0)) AS VARCHAR) + ')') 
WHEN 'numeric' THEN ('NUMERIC(' + CAST((COALESCE(c.NUMERIC_PRECISION, 0)) AS VARCHAR) + ',' + CAST((COALESCE(c.NUMERIC_SCALE, 0)) AS VARCHAR) + ')') */
WHEN 'decimal' THEN ('DECIMAL(29,' + CAST((COALESCE(c.NUMERIC_SCALE, 0)) AS VARCHAR) + ')') 
WHEN 'numeric' THEN ('NUMERIC(29,' + CAST((COALESCE(c.NUMERIC_SCALE, 0)) AS VARCHAR) + ')') 
WHEN 'nvarchar' THEN 'VARCHAR(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN '65535' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')' 
WHEN 'varchar' THEN 'VARCHAR(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN '65535' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR) END + ')' 
WHEN 'real' THEN 'REAL' 
WHEN 'tinyint' THEN 'SMALLINT' 
WHEN 'smallint' THEN 'SMALLINT' 
WHEN 'int' THEN 'INT'
WHEN 'bigint' THEN 'INT8'
WHEN 'uniqueidentifier' THEN 'CHAR(36)' 
WHEN 'xml' THEN 'VARCHAR(65535)' 
ELSE c.DATA_TYPE END + 
CASE WHEN c.IS_NULLABLE = 'NO' THEN ' NOT NULL ' ELSE '' END 
,',') + ')'
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA = '{0}' AND c.TABLE_NAME ='{1}';
            ", schemaName, tblName);

            Console.WriteLine("Start process for " + schemaName + "." + tblName);

            Connection objConnection = new ADODB.Connection();
            objConnection.ConnectionString = MSSQLConnStr;
            Recordset rs = new ADODB.Recordset();
            OleDbDataAdapter adapter = new System.Data.OleDb.OleDbDataAdapter();
            System.Data.DataTable dt = new System.Data.DataTable();

            objConnection.Open();
            rs.Open(chkIfTblExist, objConnection);
            int result = (int)rs.Fields[0].Value;
            rs.Close();

            if (result == 1)
            {
                Console.WriteLine("Found table " + schemaName + "." + tblName);
            }
            else
            {
                Console.WriteLine("Table " + schemaName + "." + tblName + " not found!");
                Console.ReadKey();
                System.Environment.Exit(1);
            }

            // get CREATE TABLE 
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

            rs.Open(getTblRowsCount, objConnection);
            if (rs.Fields[0].Value == null) { Console.WriteLine("Empty Recordset"); }
            int rowsCount = (int)rs.Fields[0].Value;
            rs.Close();
            Console.WriteLine(String.Format("Table {0}.{1} has {2} rows", schemaName, tblName, rowsCount));

            rs.Open(getDecNum, objConnection);
            while (rs.EOF != true)
            {
                scales.Add(rs.Fields[0].Value.ToString(), (int)rs.Fields[1].Value);
                rs.MoveNext();
            }            
            rs.Close();

            rs.Open(selStr, objConnection);
            if (rs.CacheSize < recordset_cache) { rs.CacheSize = (int)rowsCount; }
            else { rs.CacheSize = recordset_cache; }

            adapter.Fill(dt, rs);

            rs.Close();
            rs = null;
            objConnection.Close();

            var columns = new ParquetSharp.Column[dt.Columns.Count];

            for (int i = 0; i < dt.Columns.Count; i++)
            {
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
                            //Console.WriteLine("PRECISION = {0} SCALE = {1}", precision, scale);                            
                            columns[i] = new ParquetSharp.Column<decimal?>(dt.Columns[i].ColumnName, LogicalType.Decimal(precision, scale)); break;
                        }
                    case TypeCode.Single: columns[i] = new ParquetSharp.Column<Single?>(dt.Columns[i].ColumnName); break;
                    case TypeCode.Double: columns[i] = new ParquetSharp.Column<double?>(dt.Columns[i].ColumnName); break;
                    case TypeCode.Byte: columns[i] = new ParquetSharp.Column<Byte?>(dt.Columns[i].ColumnName); break;
                    default: Console.WriteLine("{0}, {1}", dt.Columns[i], dt.Columns[i].DataType.Name); break;
                }
            }

            using var file = new ParquetFileWriter(filePath, columns);
            using var rowGroup = file.AppendRowGroup();

            for (int i = 0; i < dt.Columns.Count; i++)
            {
                switch (System.Type.GetTypeCode(dt.Columns[i].DataType))
                {
                    case TypeCode.Int16:
                        {
                            num16Array = dt.AsEnumerable().Select(d => d.Field<Int16?>(dt.Columns[i].ColumnName)).ToArray();
                            using (var int16Writer = rowGroup.NextColumn().LogicalWriter<Int16?>())
                            {
                                int16Writer.WriteBatch(num16Array);
                            }
                            Array.Clear(num16Array, 0, num16Array.Length);
                            break;
                        }
                    case TypeCode.Int32:
                        {
                            numArray = dt.AsEnumerable().Select(d => d.Field<int?>(dt.Columns[i].ColumnName)).ToArray();
                            using (var intWriter = rowGroup.NextColumn().LogicalWriter<int?>())
                            {
                                intWriter.WriteBatch(numArray);
                            }
                            Array.Clear(numArray, 0, numArray.Length);
                            break;
                        }
                    case TypeCode.Int64:
                        {
                            num64Array = dt.AsEnumerable().Select(d => d.Field<Int64?>(dt.Columns[i].ColumnName)).ToArray();
                            using (var int64Writer = rowGroup.NextColumn().LogicalWriter<Int64?>())
                            {
                                int64Writer.WriteBatch(num64Array);
                            }
                            Array.Clear(num64Array, 0, num64Array.Length);
                            break;
                        }
                    case TypeCode.DateTime:
                        {

                            dtArray = dt.AsEnumerable().Select(d => d.Field<DateTime?>(dt.Columns[i].ColumnName)).ToArray();
                            using (var timestampWriter = rowGroup.NextColumn().LogicalWriter<DateTime?>())
                            {
                            timestampWriter.WriteBatch(dtArray);
                            }
                            Array.Clear(dtArray, 0, dtArray.Length);
                            break;
                        }    
                    case TypeCode.String:
                        {
                            strArray = dt.AsEnumerable().Select(d => d.Field<string?>(dt.Columns[i].ColumnName)).ToArray();
                            using (var strWriter = rowGroup.NextColumn().LogicalWriter<string?>())
                            {
                                strWriter.WriteBatch(strArray);
                            }
                            Array.Clear(strArray, 0, strArray.Length);
                            break;
                        }
                    case TypeCode.Boolean:
                        {
                            boolArray = dt.AsEnumerable().Select(d => d.Field<bool?>(dt.Columns[i].ColumnName)).ToArray();
                            using (var boolWriter = rowGroup.NextColumn().LogicalWriter<bool?>())
                            {
                                boolWriter.WriteBatch(boolArray);
                            }
                            Array.Clear(boolArray, 0, boolArray.Length);
                            break;
                        }
                    case TypeCode.Decimal:
                        {
                            decArray = dt.AsEnumerable().Select(d => d.Field<decimal?>(dt.Columns[i].ColumnName)).ToArray();
                            using (var decWriter = rowGroup.NextColumn().LogicalWriter<decimal?>())
                            {
                                decWriter.WriteBatch(decArray);
                            }
                            Array.Clear(decArray, 0, decArray.Length);
                            break;
                        }
                    case TypeCode.Single:
                        {
                            singlArray = dt.AsEnumerable().Select(d => d.Field<Single?>(dt.Columns[i].ColumnName)).ToArray();
                            using (var singlWriter = rowGroup.NextColumn().LogicalWriter<Single?>())
                            {
                                singlWriter.WriteBatch(singlArray);
                            }
                            Array.Clear(singlArray, 0, singlArray.Length);
                            break;
                        }
                    case TypeCode.Double:   
                        {
                            dblArray = dt.AsEnumerable().Select(d => d.Field<double?>(dt.Columns[i].ColumnName)).ToArray();
                            using (var singlWriter = rowGroup.NextColumn().LogicalWriter<double?>())
                            {
                                singlWriter.WriteBatch(dblArray);
                            }
                            Array.Clear(dblArray, 0, dblArray.Length);
                            break;
                        }     
                    case TypeCode.Byte:   
                        {
                            byteArray = dt.AsEnumerable().Select(d => d.Field<Byte?>(dt.Columns[i].ColumnName)).ToArray();
                            using (var singlWriter = rowGroup.NextColumn().LogicalWriter<Byte?>())
                            {
                                singlWriter.WriteBatch(byteArray);
                            }
                            Array.Clear(byteArray, 0, byteArray.Length);
                            break;
                        }                                        
                }
            }  //for  
            file.Close();
            Console.WriteLine("saved to '{0}'",filePath);

            Console.WriteLine("Done with " + FullTableName);
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
                get_var_value.Prepare();
                get_var_value.Parameters["var_nm"].Value = "accesskey";
                accesskeyDest = (string)get_var_value.ExecuteScalar();
                Environment.SetEnvironmentVariable("AWS_ACCESS_KEY_ID", accesskeyDest, EnvironmentVariableTarget.Process);
                get_var_value.Parameters["var_nm"].Value = "secretkey";
                secretkeyDest = (string)get_var_value.ExecuteScalar();
                Environment.SetEnvironmentVariable("AWS_SECRET_ACCESS_KEY",secretkeyDest, EnvironmentVariableTarget.Process);
                get_var_value.Parameters["var_nm"].Value = "bucket";
                bucketName = (string)get_var_value.ExecuteScalar();                
            }
        }
        private static async Task UploadFileAsync()
        {
            try
            {
                var fileTransferUtility =
                    new TransferUtility(s3Client);

                // Option 1. Upload a file. The file name is used as the object key name.
                // await fileTransferUtility.UploadAsync(filePath, bucketName);
                // Console.WriteLine("Upload 1 completed");

                // Option 2. Specify object key name explicitly.
                keyName = @"SQLTOAWS/" + Path.GetFileName(filePath);
                await fileTransferUtility.UploadAsync(filePath, bucketName, keyName);
                Console.WriteLine("Upload to s3 {0} is completed", keyName);

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
            }
            catch (Exception e)
            {
                Console.WriteLine("Unknown encountered on server. Message:'{0}' when writing an object", e.Message);
            }

        } // Task UploadFileAsync()
        static bool LoadToRedshift()
        {
            Console.WriteLine("Start Redshift COPY command");
            OdbcCommand commdest = new System.Data.Odbc.OdbcCommand();
            using (OdbcConnection connDest = new OdbcConnection(RedshiftConnStr))
            {
                connDest.Open();
                commdest.Connection = connDest;
                commdest.CommandText = sqlDropDest;
                commdest.ExecuteNonQuery(); // drop table
                commdest.CommandText = crtDestTbl;
                commdest.ExecuteNonQuery(); // create table
                string copyCommand = String.Format(@"COPY stage.{0} FROM 's3://{1}/{2}' access_key_id '{3}' secret_access_key '{4}' PARQUET ", tblName, bucketName, keyName, accesskeyDest, secretkeyDest);
                commdest.CommandText = copyCommand;
                commdest.ExecuteNonQuery();
            } // OdbcConnection
            Console.WriteLine("Finished Redshift COPY command");
            return true;
        }
    } // class Program
} // namespace sqltoaws
