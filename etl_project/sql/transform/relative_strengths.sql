{% set config = {
    "table_name": "relative_strengths",
    "source_table_name": "rates"
} %}

SELECT
    currency,
    base_currency,
    date,
    rate / LAG(rate, 1, rate) OVER (PARTITION BY currency ORDER BY date) as relative_strengths
FROM
    {{ config.source_table_name }}
WHERE
    base_currency = 'EUR'
