import logging
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, JSON
import uuid, json, os, shutil 
from datetime import datetime
import globals as gb


logger = gb.setup_logging()
engine = create_engine(gb.DB_URL)
metadata = MetaData()
Session = sessionmaker(bind=engine)
session = Session()

def read_data_file(file_path: str) -> list[dict]:
    try:
        data = []
        with open(file_path, 'r', encoding='utf-8') as lines:
            for line in lines:
                data.append(line)
        return data
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while reading the file: {e}")
    return []

def write_to_db(json_data: list[dict], table_name: str) -> None:
    try:
        tbl = Table(
            table_name, 
            metadata,
            Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True, nullable=False),
            Column('url', String),
            Column('data', JSON)
        )
              
        tbl.create(engine, checkfirst=True)
            
        for item in json_data:
            item = json.loads(item)
            existing_record = session.query(tbl).filter(tbl.c.url == item['PageURL']).first()
            if existing_record:
                existing_record_data = existing_record.data
                if existing_record_data['AdvertPrice'] != item['AdvertPrice']:
                    update_stmt = tbl.update().where(tbl.c.url == item['PageURL']).values(data=str(item))
                    session.execute(update_stmt)
            else:
                insert_stmt = tbl.insert().values(data=item, url=item['PageURL'])
                session.execute(insert_stmt)

        session.commit()
        logger.info("Data successfully written to the database.")

    except Exception as e:
        logger.error(f"Error writing data to the database: {e}")
    finally:
        session.close()

def move_file_to_archive(file_path: str, archive_dir: str) -> None:
    try:
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)
        
        destination_path = os.path.join(archive_dir, os.path.basename(file_path))
        
        if os.path.exists(destination_path):
            os.remove(destination_path)
        
        shutil.move(file_path, destination_path)
        logger.info(f"File moved to archive: {destination_path}")
    except Exception as e:
        logger.error(f"Error moving file to archive: {e}")

def run_processes(source_data_Path: str) -> None:
    try:
        for file_name in os.listdir(source_data_Path):
            if file_name.endswith('.json'):
                file_path = os.path.join(source_data_Path, file_name)
                data = read_data_file(file_path)
                write_to_db(data, gb.CARS_INFO_TABLE)
                move_file_to_archive(file_path, gb.ARCHIVE_DATA_DIR)
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")


if __name__ == '__main__':
    logger.info("Starting data loading process...")
    run_processes(gb.SOURCE_DATA_DIR)
    logger.info("Data loading process completed.")