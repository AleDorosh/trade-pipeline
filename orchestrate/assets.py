import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / 'ingest'))

from extract import fetch_and_save_trade_data
from load import load_to_db_trade_data

from dagster import asset, AssetExecutionContext, AssetKey, define_asset_job, ScheduleDefinition
from dagster_dbt import DbtCliResource, dbt_assets, DbtProject

dbt_project = DbtProject(
    project_dir=Path(__file__).parent.parent / 'transform' / 'trade_transform'
)

dbt_resource = DbtCliResource(project_dir=dbt_project, 
                              profiles_dir=str(Path.home() / '.dbt'))


@asset
def raw_trade_data():
    file_path = fetch_and_save_trade_data()
    return str(file_path)

@asset(deps=[raw_trade_data], key=AssetKey(['raw', 'raw_trades']))
def raw_trades():
    result = load_to_db_trade_data()
    return result

@dbt_assets(manifest=dbt_project.manifest_path)
def trade_transform_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(['build'], context=context).stream()

trade_pipeline_job = define_asset_job(
    name='trade_pipeline_job',
    selection=[raw_trade_data, raw_trades, trade_transform_dbt_assets]
)

trade_pipeline_schedule = ScheduleDefinition(
    job=trade_pipeline_job,
    cron_schedule='0 6 * * *',  # every day at 6:00 AM
)