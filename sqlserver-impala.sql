SET NOCOUNT ON;

-- 1. Define your table mappings here
DECLARE @Tables TABLE (
    SqlSchema NVARCHAR(128),
    SqlTable NVARCHAR(128),
    ImpalaFullTable NVARCHAR(256)
);

INSERT INTO @Tables (SqlSchema, SqlTable, ImpalaFullTable)
VALUES 
    ('dbo', 'Customers', 'prod_db.customers'),
    ('dbo', 'Orders', 'prod_db.orders'),
    ('sales', 'OrderItems', 'prod_db.order_items');

-- 2. Build dynamic SQL expressions for every table
WITH TableExpressions AS (
    SELECT 
        t.SqlSchema,
        t.SqlTable,
        t.ImpalaFullTable,
        -- SQL Server normalized columns
        STRING_AGG(
            CAST('COALESCE(TRIM(CAST(' + QUOTENAME(c.COLUMN_NAME) + ' AS NVARCHAR(MAX))), ''NULL'')' AS NVARCHAR(MAX)), 
            ', '
        ) WITHIN GROUP (ORDER BY c.COLUMN_NAME) AS SqlCols,
        -- Impala normalized columns
        STRING_AGG(
            CAST('COALESCE(TRIM(CAST(`' + LOWER(c.COLUMN_NAME) + '` AS STRING)), ''NULL'')' AS NVARCHAR(MAX)), 
            ', '
        ) WITHIN GROUP (ORDER BY c.COLUMN_NAME) AS ImpalaCols
    FROM @Tables t
    JOIN INFORMATION_SCHEMA.COLUMNS c 
        ON c.TABLE_SCHEMA = t.SqlSchema 
        AND c.TABLE_NAME = t.SqlTable
    GROUP BY t.SqlSchema, t.SqlTable, t.ImpalaFullTable
),
GeneratedQueries AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY SqlTable) AS rn,
        -- SQL Server Query Fragment
        'SELECT ''' + SqlTable + ''' AS table_name, COUNT(*) AS row_cnt, ' +
        'SUM(CAST(CONVERT(BIGINT, SUBSTRING(HASHBYTES(''SHA2_256'', CONCAT_WS(''|'', ' + SqlCols + ')), 1, 7)) AS DECIMAL(38,0))) AS table_hash ' +
        'FROM ' + QUOTENAME(SqlSchema) + '.' + QUOTENAME(SqlTable) AS SqlQuery,
        
        -- Impala Query Fragment
        'SELECT ''' + SqlTable + ''' AS table_name, COUNT(*) AS row_cnt, ' +
        'SUM(CAST(CONV(SUBSTR(SHA2(CONCAT_WS(''|'', ' + ImpalaCols + '), 256), 1, 14), 16, 10) AS DECIMAL(38,0))) AS table_hash ' +
        'FROM ' + ImpalaFullTable AS ImpalaQuery
    FROM TableExpressions
)
-- 3. Output queries cleanly as rows (avoids PRINT 4000-char truncation)
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
