"""
load.py

Reads the most recent raw JSON file from data/raw/, extracts the trade
records from the 'data' key, and loads them into a DuckDB table (raw_trades)
in data/db/trades.duckdb.

Runs as the second stage of the trade-pipeline ELT pipeline, after extract.py.
"""

import json
from pathlib import Path
import pandas as pd
import duckdb
import sys

def load_to_db_trade_data():

    folder_db = Path(__file__).parent.parent / 'data' / 'db'
    folder_db.mkdir(parents=True, exist_ok=True)
    db_name = 'trades.duckdb'
    db_path = folder_db.joinpath(db_name)

    try:
        folder_raw = Path(__file__).parent.parent / 'data' / 'raw'
        files = folder_raw.glob("*.json")
        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        if latest_file.stat().st_size == 0:
            print('File is empty')
            sys.exit('Closing script')

        with latest_file.open('r') as json_file:
            raw_data = json.load(json_file)

    except FileNotFoundError:
        print('File not found')
        sys.exit('Closing script')
    except ValueError:
            print('No .json file found')
            sys.exit('Closing script')

    try: 
        df = pd.DataFrame(raw_data['data'])

    except KeyError:
        print("Key 'data' not found in JSON response")
        sys.exit('Closing script')

    try:
        with duckdb.connect(db_path) as conn:
            conn.execute('CREATE OR REPLACE TABLE raw_trades AS SELECT * FROM df')
            print('Table raw_trades created.')    
            
    except Exception as e:
        print(f'Something went wrong. {e}')
        sys.exit('Closing script')

    return 'Total: ' + str(len(df.index)) + ' rows.'


if __name__ == "__main__":
    print(load_to_db_trade_data())
