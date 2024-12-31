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
        logger.error(f"Error in read_data_file(): {str(e)}")
        raise

def write_to_db(file_name: str, json_data: list[dict], tbl: Table) -> None:
    try:
        '''
        tbl = Table(
            table_name, 
            metadata,
            Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True, nullable=False),
            Column('url', String),
            Column('data', JSON)
        )
              
        tbl.create(engine, checkfirst=True)
        metadata.create_all(engine)
        '''
            
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
        logger.info(f"Data {os.path.basename(file_name)} successfully written to the database.")

    except Exception as e:
        logger.error(f"Error in write_to_db(): {str(e)}")
        session.close()
        raise
    finally:
        session.close()

def move_file_to_archive(file_path: str, archive_dir: str) -> None:
    try:
        if not os.path.exists(archive_dir):
            os.makedirs(archive_dir)
        
        base_name = os.path.basename(file_path)
        destination_path = os.path.join(archive_dir, base_name)
        
        if os.path.exists(destination_path):
            name, ext = os.path.splitext(base_name)
            counter = 1
            new_base_name = f"{name}_{counter}{ext}"
            new_destination_path = os.path.join(archive_dir, new_base_name)
            while os.path.exists(new_destination_path):
                counter += 1
                new_base_name = f"{name}_{counter}{ext}"
                new_destination_path = os.path.join(archive_dir, new_base_name)
            destination_path = new_destination_path
        
        shutil.move(file_path, destination_path)
        logger.info(f"The file {os.path.basename(file_path)} has been moved to {gb.ARCHIVE_DATA_DIR}")
    except Exception as e:
        logger.error(f"Error in move_file_to_archive(): {str(e)}")
        raise

def create_table(table_name: str) -> Table:
    try:
        tbl = Table(
            table_name, 
            metadata,
            Column('id', UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, unique=True, nullable=False),
            Column('url', String),
            Column('data', JSON)
        )
              
        tbl.create(engine, checkfirst=True)
        return tbl
    except Exception as e:
        logger.error(f"Error in create_table(): {str(e)}")
        raise

def execute(source_data_Path: str) -> None:
    try:
        table = create_table(gb.CARS_INFO_TABLE)
        for file_name in os.listdir(source_data_Path):
            if file_name.endswith('.json'):
                file_path = os.path.join(source_data_Path, file_name)
                data = read_data_file(file_path)
                write_to_db(file_path, data, table)
                move_file_to_archive(file_path, gb.ARCHIVE_DATA_DIR)
    except Exception as e:
        logger.error(f"Error in execute(): {str(e)}")


if __name__ == '__main__':
    logger.info("Starting data loading process...")
    execute(gb.SOURCE_DATA_DIR)
    logger.info("Data loading process completed.")