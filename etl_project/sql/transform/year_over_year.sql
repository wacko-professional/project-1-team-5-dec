{% set config = {
    "table_name": "year_over_year",
    "source_table_name": "rates_serving",
    "constraint_key": "id",
    "constraint_type": "primary",
    "reference_key":"id"
} %}

SELECT
    id,
    rate,
    LAG(rate, 365, rate) OVER (PARTITION BY currency ORDER BY date) as last_year_rate,
    ((rate - LAG(rate, 365, rate) OVER (PARTITION BY currency ORDER BY date)) / LAG(rate, 365, rate) OVER (PARTITION BY currency ORDER BY date)) * 100 as year_over_year_change
FROM
    {{ config.source_table_name }}
