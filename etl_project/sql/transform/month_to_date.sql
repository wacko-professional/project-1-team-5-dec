{% set config = {
    "table_name": "month_to_date",
    "source_table_name": "rates"
} %}


SELECT currency, rate, base_currency, date
FROM {{ config.source_table_name }}
WHERE date >= DATE_TRUNC('month', CURRENT_DATE) AND date <= CURRENT_DATE
