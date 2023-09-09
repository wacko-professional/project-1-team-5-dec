{% set config = {
    "table_name": "month_to_date",
    "source_table_name": "rates_serving",
    "constraint_key": "id",
    "constraint_type": "primary",
    "reference_key":"id"
} %}


SELECT id, date, rate
FROM {{ config.source_table_name }}
WHERE date >= DATE_TRUNC('month', CURRENT_DATE) AND date <= CURRENT_DATE
