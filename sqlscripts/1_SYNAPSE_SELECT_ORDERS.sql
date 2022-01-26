SELECT TOP 100 * FROM
    OPENROWSET(
        BULK 'https://olistdl.dfs.core.windows.net/processing/orders.parquet/*.parquet',
        FORMAT='PARQUET'
    ) AS [orders_pool]