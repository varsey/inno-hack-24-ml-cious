from src.stage import stage1, stage2
from src.utils.ch_client import ChClient
from src.utils.logger import DicLogger, LOGGING_CONFIG

log = DicLogger(LOGGING_CONFIG).log


if __name__ == '__main__':
    ch_client = ChClient()

    data_collection = ch_client.get_data_from_ch()
    processed_data = stage1.stage1_run(data_collection)
    pairs = stage2.stage2_run(processed_data)
    log.info(f'{pairs.shape}')

    ch_client.send_data_to_ch(pairs)
    ch_client.client.disconnect()

    log.info('Done')
