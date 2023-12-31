{% set config = {
    "table_name": "moving_averages",
    "source_table_name": "rates"
} %}

SELECT
    currency,
    base_currency,
    date,
    AVG(rate) OVER (PARTITION BY currency ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as seven_day_moving_average,
    AVG(rate) OVER (PARTITION BY currency ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as thirty_day_moving_average,
    AVG(rate) OVER (PARTITION BY currency ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) as ninety_day_moving_average
FROM
    {{ config.source_table_name }}
