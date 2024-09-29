from src.stage import stage1, stage2
from src.utils.logger import DicLogger, LOGGING_CONFIG

log = DicLogger(LOGGING_CONFIG).log

if __name__ == '__main__':
    processed_data = stage1.stage1_run()
    stage2.stage2_run(processed_data)
