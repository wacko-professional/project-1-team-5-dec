{% set config = {
    "table_name": "moving_averages",
    "source_table_name": "rates"
} %}

SELECT
    currency,
    base_currency,
    date,
    AVG(rate) OVER (PARTITION BY currency ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as moving_average
FROM
    {{ config.source_table_name }}
