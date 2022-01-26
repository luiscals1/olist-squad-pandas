SELECT    
    YEAR(order_purchase_timestamp) AS current_year,
    COUNT(*) AS orders_per_year

FROM
    OPENROWSET(
        BULK 'https://olistdl.dfs.core.windows.net/processing/orders.parquet/*.parquet',
        FORMAT='PARQUET'
    ) AS [orders_year_pool]
--WHERE orders_year_pool.filepath(1) >= '2016' AND orders_years_pool.filepath(1) <= '2018'
GROUP BY YEAR(order_purchase_timestamp)
ORDER BY 1 DESC