-- "What are Estonia's top export commodities by total trade value, 
-- and which countries receive them?"
SELECT
    commodity_code,
    SUM(trade_value_USD) AS total_trade_value,
    partner_country
FROM {{ ref('stg_trades') }}
WHERE partner_country != 0
GROUP BY commodity_code, partner_country
ORDER BY total_trade_value DESC