'''
extract.py

Fetches annual trade export data from the UN Comtrade public API
and saves it as a raw JSON snapshot to data/raw/.

Runs as the first stage of the trade-pipeline ELT pipeline.
'''

import requests
import json
from pathlib import Path
import datetime
import sys

COUNTRY = '233' #Estonia
YEAR = '2025'
API_URL = f'https://comtradeapi.un.org/public/v1/preview/C/A/HS?reporterCode={COUNTRY}&flowCode=X&period={YEAR}'


def fetch_and_save_trade_data():
    folder = Path(__file__).parent.parent / 'data' / 'raw'
    folder.mkdir(parents=True, exist_ok=True)

    today_date = datetime.datetime.now().strftime('%y-%m-%d')
    file_name = 'comtrade_' + today_date + '.json'
    file_path = folder.joinpath(file_name)

    try:
        response = requests.get(API_URL, timeout=10)
        status_code = response.status_code
    except requests.exceptions.RequestException as exc:
        print('Something went wrong while connecting to the API')
        print(exc.args[0])
        sys.exit('Closing script')

    if status_code == 401:
        print('Unauthorized — check your API key')
        sys.exit('Closing script')
    elif status_code == 404:
        print('Not found — check the endpoint URL')
        sys.exit('Closing script')
    elif status_code == 429:
        print('Rate limited — slow down your requests')
        sys.exit('Closing script')

    elif status_code == 200:
        try:
            
            new_data = response.json()
            # Rename existing file rather than overwrite — preserves previous snapshot
            if file_path.exists():
                old_path = file_path.parent / (file_path.stem + '_old' + file_path.suffix)
                if old_path.exists():
                    counter = 1
                    while old_path.exists():
                        old_path = file_path.parent / (file_path.stem + f'_old{counter}' + file_path.suffix)
                        counter += 1
                print(f'File already exists, renaming to {old_path.name}')
                file_path.rename(old_path)

            with file_path.open('w') as json_file:
                json.dump(new_data, json_file, indent=4)
                print(f'Data written to {file_path}.')

        except json.JSONDecodeError:
            print('Invalid JSON response!')
            print(f'Raw content received: {response.text[:200]}')
            sys.exit('Closing script')

        except OSError as err:
            print('Something went wrong: ', err)
            sys.exit('Closing script')

    else:
        print(f'HTTP Error {status_code}')
        sys.exit('Closing script')

    return file_path

if __name__ == "__main__":
    fetch_and_save_trade_data()
