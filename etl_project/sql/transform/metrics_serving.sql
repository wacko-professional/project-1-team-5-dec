{% set config = {
    "table_name": "metrics_serving",
    "source_table_name": "metrics_staging"
} %}


SELECT
		date,
		currency_pair,
		rate,
		seven_day_average,
		thirty_day_average,
		ninety_day_average,
		mtd_average,
		ytd_average,
		yoy,
		thirty_day_volatility,
		thirty_day_rsi AS thirty_day_relative_strength_index
	FROM 
		{{ config.source_table_name }}