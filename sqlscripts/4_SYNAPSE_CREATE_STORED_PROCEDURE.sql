CREATE OR ALTER PROC CreateSQLServerlessOrdersView
AS
BEGIN

DECLARE @statement VARCHAR(MAX) = 
'
CREATE OR ALTER VIEW olistOrders AS
SELECT
    *
FROM
    OPENROWSET(
        BULK ''processing/orders.parquet/*.parquet'',
        DATA_SOURCE = ''OlistDataLake'',
        FORMAT=''PARQUET''
    ) AS ord;
'
PRINT @statement

EXEC (@statement)

END
GO

EXEC CreateSQLServerlessOrdersView

SELECT * FROM olistOrders