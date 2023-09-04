{% set config = {
    "table_name": "top_volatility_serving",
    "source_table_name": "metrics_staging"
} %}


SELECT 
		date,
		currency_pair AS top_relative_strength_pair,
		ROUND(thirty_day_rsi::numeric,2) AS thirty_day_relative_strength_index
	FROM 
		{{ config.source_table_name }}
	WHERE
		rank_rsi = 1
	ORDER BY
		date DESC;