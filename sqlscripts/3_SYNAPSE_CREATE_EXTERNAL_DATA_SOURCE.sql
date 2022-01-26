-- Drop database if it exists
DROP DATABASE IF EXISTS olist_synapse
GO

-- Create new database
CREATE DATABASE [olist_synapse];
GO

use olist_synapse
GO

-- There is no credential in data surce. We are using public storage account which doesn't need a secret.
CREATE EXTERNAL DATA SOURCE OlistDataLake
WITH ( LOCATION = 'https://olistdl.dfs.core.windows.net/')
GO

DROP VIEW IF EXISTS olistOrders;
GO

CREATE VIEW olistOrders AS
SELECT
    *
FROM
    OPENROWSET(
        BULK 'processing/orders.parquet/*.parquet',
        DATA_SOURCE = 'OlistDataLake',
        FORMAT='PARQUET'
    ) AS ord;

GO

SELECT * FROM olistOrders
GO