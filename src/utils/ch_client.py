import pandas as pd
from clickhouse_driver import Client

from src.utils.logger import DicLogger, LOGGING_CONFIG

log = DicLogger(LOGGING_CONFIG).log

def run_ch_task():
    client = Client(host='clickhouse')
    log.info(f'{client.execute("SHOW DATABASES")}')
    data = client.execute('SELECT * FROM table_dataset1')
    log.info(f'{data}')
    log.info('***')
    if data:
        log.info(f'{pd.DataFrame(data[1:], columns=data[0]).head(10)}')
