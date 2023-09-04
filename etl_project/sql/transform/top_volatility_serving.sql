{% set config = {
    "table_name": "top_volatility_serving",
    "source_table_name": "metrics_staging"
} %}


SELECT 
		date,
		currency_pair AS top_volatility_pair,
		ROUND(thirty_day_volatility::numeric,2) AS thirty_day_volatility
	FROM 
		{{ config.source_table_name }}
	WHERE
		rank_volatility = 1
	ORDER BY
		date DESC