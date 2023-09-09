{% set config = {
    "table_name": "rates_serving",
    "source_table_name": "rates",
    "constraint_key": "id",
    "constraint_type": "primary"
} %}

SELECT
    dense_rank() over (order by date, base_currency, currency) as id, 
    date, 
    base_currency, 
    currency, 
    rate
FROM
    {{ config.source_table_name }}
