import pandas as pd
from clickhouse_driver import Client

from src.utils.logger import DicLogger, LOGGING_CONFIG

log = DicLogger(LOGGING_CONFIG).log

def get_data_from_ch() -> tuple:
    client = Client(host='localhost')
    log.info(f'{client.execute("SHOW DATABASES")}')

    # TO-DO figure out how to get column names from ch
    df1 = client.execute('SELECT * FROM table_dataset1')
    df1_clms = client.execute('SELECT * FROM table_dataset1', with_column_types=True)

    df2 = client.execute('SELECT * FROM table_dataset2')
    df2_clms = client.execute('SELECT * FROM table_dataset2', with_column_types=True)

    df3 = client.execute('SELECT * FROM table_dataset3')
    df3_clms = client.execute('SELECT * FROM table_dataset3', with_column_types=True)
    return (df1, df1_clms), (df2, df2_clms), (df3, df3_clms)
