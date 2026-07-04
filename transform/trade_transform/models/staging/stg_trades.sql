SELECT
    reporterCode AS reporting_country,
    partnerCode AS partner_country,
    flowCode AS trade_direction,
    period AS trade_year,
    cmdCode AS commodity_code,
    primaryValue AS trade_value_USD,
    netWgt AS goods_weight
FROM {{ ref('raw_trades_flattened') }}