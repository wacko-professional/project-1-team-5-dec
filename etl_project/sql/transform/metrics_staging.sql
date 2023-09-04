{% set config = {
    "table_name": "metrics_staging",
    "source_table_name": "rates"
} %}


WITH initial_query AS 
(
SELECT
        concat(base_currency,'/',currency) AS currency_pair,
        date,
		rate,
        CASE WHEN COUNT(*) OVER (PARTITION BY concat(base_currency,'/',currency) ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) >= 7 THEN 
		AVG(rate) OVER (
            PARTITION BY concat(base_currency,'/',currency)
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) ELSE NULL END AS seven_day_average,
        CASE WHEN COUNT(*) OVER (PARTITION BY concat(base_currency,'/',currency) ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 30 THEN
		AVG(rate) OVER (
            PARTITION BY concat(base_currency,'/',currency)
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) ELSE NULL END AS thirty_day_average,
        CASE WHEN COUNT(*) OVER (PARTITION BY concat(base_currency,'/',currency) ORDER BY date ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) >= 90 THEN
		AVG(rate) OVER (
            PARTITION BY concat(base_currency,'/',currency)
            ORDER BY date
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) ELSE NULL END AS ninety_day_average,
		AVG(rate) OVER (
			PARTITION BY concat(base_currency,'/',currency), TO_CHAR(date, 'YYYY-MM')
		) AS mtd_average,
		AVG(rate) OVER (
			PARTITION BY concat(base_currency,'/',currency), TO_CHAR(date, 'YYYY')
		) AS ytd_average,
		CASE WHEN COUNT(*) OVER (PARTITION BY concat(base_currency,'/',currency) ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 30 THEN
		STDDEV_POP(rate) OVER (
				PARTITION BY concat(base_currency,'/',currency)
				ORDER BY date
				ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
		) ELSE NULL END AS thirty_day_volatility,
		LAG(rate, 365) OVER (
			PARTITION BY concat(base_currency,'/',currency)
			ORDER BY date
		) AS exchange_rate_1_year_ago,
		CASE
			WHEN COUNT(*) OVER (PARTITION BY concat(base_currency,'/',currency) ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 30 THEN
				CASE
					WHEN rate > LAG(rate, 1) OVER (PARTITION BY concat(base_currency,'/',currency) ORDER BY date) THEN rate - LAG(rate, 1) OVER (PARTITION BY concat(base_currency,'/',currency) ORDER BY date)
					ELSE 0
				END
			ELSE NULL
		END AS gain,
		CASE
			WHEN COUNT(*) OVER (PARTITION BY concat(base_currency,'/',currency) ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) >= 30 THEN
				CASE
					WHEN rate < LAG(rate, 1) OVER (PARTITION BY concat(base_currency,'/',currency) ORDER BY date) THEN LAG(rate, 1) OVER (PARTITION BY concat(base_currency,'/',currency) ORDER BY date) - rate
					ELSE 0
				END
			ELSE NULL
		END AS loss
    FROM
        {{ config.source_table_name }}
), second_query AS 
(
SELECT 
	currency_pair, 
	date, 
	rate, 
	seven_day_average, 
	thirty_day_average,
	ninety_day_average,
	mtd_average,
	ytd_average,
	thirty_day_volatility,
	exchange_rate_1_year_ago,
	(rate - exchange_rate_1_year_ago) / exchange_rate_1_year_ago AS yoy,
	gain,
	loss,
	CASE
        WHEN SUM(loss) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) = 0 THEN NULL
        WHEN SUM(gain) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) = 0 THEN NULL
		ELSE
            CASE
                WHEN SUM(gain) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) IS NOT NULL 
				AND SUM(loss) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) IS NOT NULL THEN
                    100 - (100 / (1 + (SUM(gain) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) / SUM(loss) OVER (PARTITION BY currency_pair ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW))))
                ELSE NULL
            END
    END AS thirty_day_rsi
FROM
	initial_query
ORDER BY 
	date DESC, currency_pair
), third_query AS 
(
SELECT 
	currency_pair, 
	date, 
	rate, 
	seven_day_average, 
	thirty_day_average,
	ninety_day_average,
	mtd_average,
	ytd_average,
	thirty_day_volatility,
	yoy,
	thirty_day_rsi,
	CASE WHEN thirty_day_rsi IS NULL THEN NULL ELSE DENSE_RANK() OVER (PARTITION BY date ORDER BY thirty_day_rsi DESC NULLS LAST) END AS rank_rsi,
	CASE WHEN thirty_day_volatility IS NULL THEN NULL ELSE DENSE_RANK() OVER (PARTITION BY date ORDER BY thirty_day_volatility DESC NULLS LAST) END AS rank_volatility
FROM 
	second_query
ORDER BY 
	date DESC, currency_pair
)
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
	thirty_day_rsi,
	rank_volatility,
	rank_rsi
FROM third_query