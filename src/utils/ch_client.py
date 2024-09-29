from clickhouse_driver import Client

from src.utils.decorators import duration
from src.utils.logger import DicLogger, LOGGING_CONFIG

log = DicLogger(LOGGING_CONFIG).log

class ChClient():
    def __init__(self):
        self.client = Client(host='clickhouse') # change to 'localhost' if not running in docker

    @duration
    def get_data_from_ch(self) -> tuple:
        LIMIT = 1_000_000
        log.info(f'{self.client.execute("SHOW DATABASES")}')

        # TO-DO figure out how to get column names from ch
        df1 = self.client.execute('SELECT * FROM table_dataset1 LIMIT 1000000')
        df1_clms = self.client.execute('SELECT * FROM table_dataset1 LIMIT 1', with_column_types=True)

        df2 = self.client.execute('SELECT * FROM table_dataset2 LIMIT 1000000')
        df2_clms = self.client.execute('SELECT * FROM table_dataset2 LIMIT 1', with_column_types=True)

        df3 = self.client.execute('SELECT * FROM table_dataset3 LIMIT 1000000')
        df3_clms = self.client.execute('SELECT * FROM table_dataset3 LIMIT 1', with_column_types=True)

        return (df1[:LIMIT], df1_clms), (df2[:LIMIT], df2_clms), (df3[:LIMIT], df3_clms)

    @duration
    def send_data_to_ch(self, res) -> None:

        data_to_insert = []
        for index, row in res.iterrows():
            data_to_insert.append((row[1], row[2], row[3]))

        self.client.execute('INSERT INTO table_results (id_is1, id_is2, id_is3) VALUES', data_to_insert)

