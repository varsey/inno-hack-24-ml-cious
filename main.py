from src.stage import stage1, stage2
from src.utils.ch_client import run_ch_task
from src.utils.logger import DicLogger, LOGGING_CONFIG

log = DicLogger(LOGGING_CONFIG).log


if __name__ == '__main__':
    run_ch_task()
    processed_data = stage1.stage1_run()
    stage2.stage2_run(processed_data)
