{% set config = {
    "table_name": "volatility",
    "source_table_name": "rates"
} %}

WITH initial_query AS (
SELECT
    currency,
    base_currency,
    date,
    STDDEV(rate) OVER (PARTITION BY currency ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as volatility
FROM
    {{ config.source_table_name }}
)
SELECT 
    currency,
    base_currency,
    date,
    volatility,
    CASE WHEN volatility IS NULL THEN NULL ELSE DENSE_RANK() OVER (PARTITION BY date ORDER BY volatility DESC NULLS LAST) END AS rank_volatility
FROM initial_query
