{% set config = {
    "table_name": "volatility",
    "source_table_name": "rates"
} %}

SELECT
    currency,
    base_currency,
    date,
    STDDEV(rate) OVER (PARTITION BY currency ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as volatility
FROM
    {{ config.source_table_name }}
