{% if target.name == 'dev' %}

SELECT * FROM {{ source('raw', 'raw_trades') }}

{% else %}

SELECT
    raw_data:reporterCode::VARCHAR AS reporterCode,
    raw_data:partnerCode::VARCHAR AS partnerCode,
    raw_data:flowCode::VARCHAR AS flowCode,
    raw_data:period::VARCHAR AS period,
    raw_data:cmdCode::VARCHAR AS cmdCode,
    raw_data:primaryValue::FLOAT AS primaryValue,
    raw_data:netWgt::FLOAT AS netWgt
FROM {{ source('raw', 'raw_trades') }}

{% endif %}