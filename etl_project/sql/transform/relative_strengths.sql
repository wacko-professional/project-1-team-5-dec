{% set config = {
    "table_name": "relative_strengths",
    "source_table_name": "rates"
} %}

WITH initial_query AS (
SELECT
    currency,
    base_currency,
    date,
    rate / LAG(rate, 1, rate) OVER (PARTITION BY currency ORDER BY date) as relative_strengths
FROM
    {{ config.source_table_name }}
WHERE
    base_currency = 'EUR'
)
SELECT 
    currency,
    base_currency,
    date,
    relative_strengths,
    CASE WHEN relative_strengths IS NULL THEN NULL ELSE DENSE_RANK() OVER (PARTITION BY date ORDER BY relative_strengths DESC NULLS LAST) END AS rank_rs
FROM
    initial_query
WHERE
    base_currency = 'EUR'
