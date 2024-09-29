from src.stage import stage1, stage2
from src.utils.ch_client import get_data_from_ch
from src.utils.logger import DicLogger, LOGGING_CONFIG

log = DicLogger(LOGGING_CONFIG).log


if __name__ == '__main__':
    data_collection = get_data_from_ch()
    processed_data = stage1.stage1_run(data_collection)
    stage2.stage2_run(processed_data)
