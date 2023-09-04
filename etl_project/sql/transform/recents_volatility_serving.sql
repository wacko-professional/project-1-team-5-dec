{% set config = {
    "table_name": "recents_volatility_serving",
    "source_table_name": "metrics_staging"
} %}


WITH initial_query AS (
	SELECT currency_pair, DENSE_RANK() OVER (PARTITION BY currency_pair ORDER BY date DESC) AS date_rank, date, thirty_day_volatility
	FROM {{ config.source_table_name }}
	WHERE rank_volatility = 1
)

SELECT currency_pair, date AS latest_top_volatility_date, thirty_day_volatility AS latest_top_volatility_value
FROM initial_query
WHERE date_rank = 1