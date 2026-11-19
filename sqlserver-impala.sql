SET NOCOUNT ON;

DECLARE @Tables TABLE (
    SqlSchema NVARCHAR(128),
    SqlTable NVARCHAR(128),
    ImpalaFullTable NVARCHAR(256)
);

INSERT INTO @Tables (SqlSchema, SqlTable, ImpalaFullTable)
VALUES 
    ('dbo', 'Customers', 'prod_db.customers'),
    ('dbo', 'Orders', 'prod_db.orders');

WITH ColumnRules AS (
    SELECT 
        t.SqlSchema,
        t.SqlTable,
        t.ImpalaFullTable,
        c.COLUMN_NAME,
        -- SQL Server: Force VARCHAR (UTF-8/ASCII bytes) and standardize dates/floats
        CASE 
            WHEN c.DATA_TYPE IN ('datetime', 'datetime2', 'smalldatetime', 'datetimeoffset') 
                THEN 'COALESCE(FORMAT(' + QUOTENAME(c.COLUMN_NAME) + ', ''yyyy-MM-dd HH:mm:ss''), ''NULL'')'
            WHEN c.DATA_TYPE = 'date' 
                THEN 'COALESCE(FORMAT(' + QUOTENAME(c.COLUMN_NAME) + ', ''yyyy-MM-dd''), ''NULL'')'
            WHEN c.DATA_TYPE IN ('float', 'real') 
                THEN 'COALESCE(CAST(CAST(' + QUOTENAME(c.COLUMN_NAME) + ' AS DECIMAL(18,4)) AS VARCHAR(50)), ''NULL'')'
            ELSE 'COALESCE(TRIM(CAST(' + QUOTENAME(c.COLUMN_NAME) + ' AS VARCHAR(MAX))), ''NULL'')'
        END AS SqlColExpr,
        -- Impala: Match exact formatting rules
        CASE 
            WHEN c.DATA_TYPE IN ('datetime', 'datetime2', 'smalldatetime', 'datetimeoffset') 
                THEN 'COALESCE(FROM_UNIXTIME(UNIX_TIMESTAMP(`' + LOWER(c.COLUMN_NAME) + '`), ''yyyy-MM-dd HH:mm:ss''), ''NULL'')'
            WHEN c.DATA_TYPE = 'date' 
                THEN 'COALESCE(SUBSTR(CAST(`' + LOWER(c.COLUMN_NAME) + '` AS STRING), 1, 10), ''NULL'')'
            WHEN c.DATA_TYPE IN ('float', 'real') 
                THEN 'COALESCE(CAST(CAST(`' + LOWER(c.COLUMN_NAME) + '` AS DECIMAL(18,4)) AS STRING), ''NULL'')'
            ELSE 'COALESCE(TRIM(CAST(`' + LOWER(c.COLUMN_NAME) + '` AS STRING)), ''NULL'')'
        END AS ImpalaColExpr
    FROM @Tables t
    JOIN INFORMATION_SCHEMA.COLUMNS c 
        ON c.TABLE_SCHEMA = t.SqlSchema 
        AND c.TABLE_NAME = t.SqlTable
    WHERE c.DATA_TYPE NOT IN ('varbinary', 'binary', 'image', 'xml')
),
TableExpressions AS (
    SELECT 
        SqlSchema,
        SqlTable,
        ImpalaFullTable,
        STRING_AGG(CAST(SqlColExpr AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY COLUMN_NAME) AS SqlCols,
        STRING_AGG(CAST(ImpalaColExpr AS VARCHAR(MAX)), ', ') WITHIN GROUP (ORDER BY COLUMN_NAME) AS ImpalaCols
    FROM ColumnRules
    GROUP BY SqlSchema, SqlTable, ImpalaFullTable
),
GeneratedQueries AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SqlTable) AS rn,
        -- Prepend 0x00 to 7 bytes (8 bytes total) so BIGINT is always unsigned/positive
        'SELECT ''' + SqlTable + ''' AS table_name, COUNT(*) AS row_cnt, ' +
        'SUM(CAST(CONVERT(BIGINT, 0x00 + SUBSTRING(HASHBYTES(''SHA2_256'', CONCAT_WS(''|'', ' + SqlCols + ')), 1, 7)) AS DECIMAL(38,0))) AS table_hash ' +
        'FROM ' + QUOTENAME(SqlSchema) + '.' + QUOTENAME(SqlTable) AS SqlQuery,
        
        'SELECT ''' + SqlTable + ''' AS table_name, COUNT(*) AS row_cnt, ' +
        'SUM(CAST(CONV(SUBSTR(SHA2(CONCAT_WS(''|'', ' + ImpalaCols + '), 256), 1, 14), 16, 10) AS DECIMAL(38,0))) AS table_hash ' +
        'FROM ' + ImpalaFullTable AS ImpalaQuery
    FROM TableExpressions
)
SELECT 
    'SQL_SERVER' AS TargetPlatform,
    CASE WHEN rn > 1 THEN 'UNION ALL ' ELSE '' END + SqlQuery + 
    CASE WHEN rn = MAX(rn) OVER() THEN ' ORDER BY table_name;' ELSE '' END AS GeneratedSQL
FROM GeneratedQueries
UNION ALL
SELECT 
    'IMPALA' AS TargetPlatform,
    CASE WHEN rn > 1 THEN 'UNION ALL ' ELSE '' END + ImpalaQuery + 
    CASE WHEN rn = MAX(rn) OVER() THEN ' ORDER BY table_name;' ELSE '' END AS GeneratedSQL
FROM GeneratedQueries;






DECLARE @SqlSchema sysname = N'dbo';
DECLARE @SqlTable sysname = N'YourSqlTable';
DECLARE @ImpalaDb sysname = N'your_db';
DECLARE @ImpalaTable sysname = N'your_impala_table';
DECLARE @Expr nvarchar(max);
SELECT @Expr = STRING_AGG(
    N'CASE WHEN `' + LOWER(COLUMN_NAME) + N'` IS NULL THEN CONCAT(CHR(92),''N'') ELSE ' +
    N'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`' + LOWER(COLUMN_NAME) + N'` AS STRING),' +
    N'CHR(92),CONCAT(CHR(92),CHR(92))),CHR(124),CONCAT(CHR(92),CHR(124))),' +
    N'CHR(10),CONCAT(CHR(92),''n'')),CHR(13),CONCAT(CHR(92),''r'')),' +
    N'CHR(9),CONCAT(CHR(92),''t'')) END',
    N',' + CHAR(13) + CHAR(10) + N'        '
) WITHIN GROUP (ORDER BY LOWER(COLUMN_NAME) COLLATE Latin1_General_100_BIN2)
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = @SqlSchema
  AND TABLE_NAME = @SqlTable;
SELECT
    N'SELECT' + CHAR(13) + CHAR(10) +
    N'    CONCAT_WS(' + CHAR(13) + CHAR(10) +
    N'        CHR(124),' + CHAR(13) + CHAR(10) +
    N'        ' + @Expr + CHAR(13) + CHAR(10) +
    N'    ) AS canon' + CHAR(13) + CHAR(10) +
    N'FROM `' + @ImpalaDb + N'`.`' + @ImpalaTable + N'`;' AS GeneratedImpalaSql;



SELECT
    COUNT(*) AS row_cnt,
    SUM(
        CASE
            WHEN length(canon) <> length(regexp_replace(canon, '[^ -~]', '')) THEN 1
            ELSE 0
        END
    ) AS bad_rows,
    COALESCE(
        SUM(CAST(CONV(SUBSTR(LOWER(SHA2(canon, 256)), 1, 14), 16, 10) AS DECIMAL(38,0))),
        0
    ) AS fp1,
    COALESCE(
        SUM(CAST(CONV(SUBSTR(LOWER(SHA2(canon, 256)), 15, 14), 16, 10) AS DECIMAL(38,0))),
        0
    ) AS fp2,
    COALESCE(
        SUM(CAST(CONV(SUBSTR(LOWER(SHA2(canon, 256)), 29, 14), 16, 10) AS DECIMAL(38,0))),
        0
    ) AS fp3,
    COALESCE(
        SUM(CAST(CONV(SUBSTR(LOWER(SHA2(canon, 256)), 43, 14), 16, 10) AS DECIMAL(38,0))),
        0
    ) AS fp4
FROM (
    SELECT
        CONCAT_WS(
            CHR(124),
            CASE
                WHEN `col1` IS NULL THEN CONCAT(CHR(92), 'N')
                ELSE REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    CAST(`col1` AS STRING),
                                    CHR(92), CONCAT(CHR(92), CHR(92))
                                ),
                                CHR(124), CONCAT(CHR(92), CHR(124))
                            ),
                            CHR(10), CONCAT(CHR(92), 'n')
                        ),
                        CHR(13), CONCAT(CHR(92), 'r')
                    ),
                    CHR(9), CONCAT(CHR(92), 't')
                )
            END,
            CASE
                WHEN `col2` IS NULL THEN CONCAT(CHR(92), 'N')
                ELSE REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    CAST(`col2` AS STRING),
                                    CHR(92), CONCAT(CHR(92), CHR(92))
                                ),
                                CHR(124), CONCAT(CHR(92), CHR(124))
                            ),
                            CHR(10), CONCAT(CHR(92), 'n')
                        ),
                        CHR(13), CONCAT(CHR(92), 'r')
                    ),
                    CHR(9), CONCAT(CHR(92), 't')
                )
            END
        ) AS canon
    FROM `your_db`.`your_impala_table`
) AS t;



DECLARE @Tables TABLE (SqlSchema sysname, SqlTable sysname, ImpalaDb sysname, ImpalaTable sysname);
INSERT @Tables (SqlSchema, SqlTable, ImpalaDb, ImpalaTable)
VALUES
    (N'dbo', N'Orders', N'warehouse', N'orders'),
    (N'dbo', N'Customers', N'warehouse', N'customers');
WITH E AS (
    SELECT
        t.SqlSchema,
        t.SqlTable,
        t.ImpalaDb,
        t.ImpalaTable,
        SqlExpr = STRING_AGG(
            N'CASE WHEN ' + QUOTENAME(c.COLUMN_NAME) + N' IS NULL THEN N''\N'' ELSE ' +
            N'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CONVERT(nvarchar(max), ' + QUOTENAME(c.COLUMN_NAME) + N'), CHAR(92), CHAR(92)+CHAR(92)), ''|'', CHAR(92)+''|''), CHAR(10), CHAR(92)+''n''), CHAR(13), CHAR(92)+''r''), CHAR(9), CHAR(92)+''t'') END',
            N', '
        ) WITHIN GROUP (ORDER BY LOWER(c.COLUMN_NAME) COLLATE Latin1_General_100_BIN2),
        ImpalaExpr = STRING_AGG(
            N'CASE WHEN `' + LOWER(c.COLUMN_NAME) + N'` IS NULL THEN CONCAT(CHR(92),''N'') ELSE ' +
            N'REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(CAST(`' + LOWER(c.COLUMN_NAME) + N'` AS STRING), CHR(92), CONCAT(CHR(92),CHR(92))), CHR(124), CONCAT(CHR(92),CHR(124))), CHR(10), CONCAT(CHR(92),''n'')), CHR(13), CONCAT(CHR(92),''r'')), CHR(9), CONCAT(CHR(92),''t'')) END',
            N', '
        ) WITHIN GROUP (ORDER BY LOWER(c.COLUMN_NAME) COLLATE Latin1_General_100_BIN2)
    FROM @Tables AS t
    JOIN INFORMATION_SCHEMA.COLUMNS AS c
        ON c.TABLE_SCHEMA = t.SqlSchema
       AND c.TABLE_NAME = t.SqlTable
    GROUP BY t.SqlSchema, t.SqlTable, t.ImpalaDb, t.ImpalaTable
)
SELECT
    TableName = E.SqlSchema + N'.' + E.SqlTable,
    GeneratedSqlServerSql =
        N'WITH r AS (SELECT CONCAT_WS(CHAR(124), ' + E.SqlExpr + N') AS canon FROM ' + QUOTENAME(E.SqlSchema) + N'.' + QUOTENAME(E.SqlTable) + N'), ' +
        N'b AS (SELECT canon, CASE WHEN PATINDEX(N''%[^ -~]%'', canon COLLATE Latin1_General_100_BIN2) > 0 THEN 1 ELSE 0 END bad FROM r), ' +
        N'h AS (SELECT HASHBYTES(''SHA2_256'', CONVERT(varbinary(max), CONVERT(varchar(max), canon COLLATE Latin1_General_100_BIN2_UTF8))) d, bad FROM b) ' +
        N'SELECT COUNT(*) row_cnt, SUM(bad) bad_rows, ' +
        N'COALESCE(SUM(CONVERT(decimal(38,0), CONVERT(bigint, SUBSTRING(d, 1, 7)))), 0) fp1, ' +
        N'COALESCE(SUM(CONVERT(decimal(38,0), CONVERT(bigint, SUBSTRING(d, 8, 7)))), 0) fp2, ' +
        N'COALESCE(SUM(CONVERT(decimal(38,0), CONVERT(bigint, SUBSTRING(d, 15, 7)))), 0) fp3, ' +
        N'COALESCE(SUM(CONVERT(decimal(38,0), CONVERT(bigint, SUBSTRING(d, 22, 7)))), 0) fp4 FROM h;',
    GeneratedImpalaSql =
        N'WITH r AS (SELECT CONCAT_WS(CHR(124), ' + E.ImpalaExpr + N') AS canon FROM `' + E.ImpalaDb + N'`.`' + E.ImpalaTable + N'`), ' +
        N'b AS (SELECT canon, CASE WHEN length(canon) <> length(regexp_replace(canon, ''[^ -~]'', '''')) THEN 1 ELSE 0 END bad FROM r), ' +
        N'h AS (SELECT lower(SHA2(canon, 256)) hx, bad FROM b) ' +
        N'SELECT COUNT(*) row_cnt, SUM(bad) bad_rows, ' +
        N'COALESCE(SUM(CAST(CONV(SUBSTR(hx, 1, 14), 16, 10) AS DECIMAL(38,0))), 0) fp1, ' +
        N'COALESCE(SUM(CAST(CONV(SUBSTR(hx, 15, 14), 16, 10) AS DECIMAL(38,0))), 0) fp2, ' +
        N'COALESCE(SUM(CAST(CONV(SUBSTR(hx, 29, 14), 16, 10) AS DECIMAL(38,0))), 0) fp3, ' +
        N'COALESCE(SUM(CAST(CONV(SUBSTR(hx, 43, 14), 16, 10) AS DECIMAL(38,0))), 0) fp4 FROM h;'
FROM E
ORDER BY E.SqlSchema, E.SqlTable;
