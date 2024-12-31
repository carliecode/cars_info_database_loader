import logging
from datetime import datetime

DB_URL = 'postgresql://postgres:postgres@localhost:5432/autochek'
SOURCE_DATA_DIR = '../cars_info_proj_data/source' 
ARCHIVE_DATA_DIR = '../cars_info_proj_data/archive'
CARS_INFO_TABLE = 'src_cars_info'

current_time = datetime.now().strftime("%Y%m%d%H%M%S")
log_file = f"logs/cars_info_loader_{current_time}.log"

def setup_logging(log_file=log_file, level=logging.INFO)-> logging.Logger:
    logger = logging.getLogger(__name__)
    logger.setLevel(level)    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)    
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger