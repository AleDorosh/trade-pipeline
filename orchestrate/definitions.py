from dagster import Definitions
from assets import raw_trade_data, raw_trades, trade_transform_dbt_assets, dbt_resource, trade_pipeline_job, trade_pipeline_schedule


defs = Definitions(
    assets=[raw_trade_data, raw_trades, trade_transform_dbt_assets],
    resources={'dbt': dbt_resource},
    jobs=[trade_pipeline_job],
    schedules=[trade_pipeline_schedule]
)