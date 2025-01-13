import os
import globals as gb
import big_query_scripts as bq
from google.cloud import bigquery, storage

logger = gb.setup_logging()

def read_csv_from_gcs(bucket_name: str, file_name: str) -> list[str]:
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        data = blob.download_as_string().decode('utf-8')
        lines = data.split('\n')
        return [line for line in lines if line]
    except Exception as e:
        logger.error(f"Error in read_csv_from_gcs(): {str(e)}")
        raise

def read_files_from_bucket(bucket_name: str) -> list[str]:
    try:
        records = []
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        for file in bucket.list_blobs():
            data = read_csv_from_gcs(bucket_name, file.name)
            records.extend(data)
        return records
    except Exception as e:
        logger.error(f"Error in read_files_from_bucket(): {str(e)}")
        raise

def load_bigquery_source_table(dataset_name: str, table_name: str, records: list[str]) -> bool:
    response = False
    try:
        client = bigquery.Client()
        table_id = f'{client.project}.{dataset_name}.{table_name}'
        query = f"SELECT car_json_string FROM `{table_id}`"

        existing_data = client.query(query).result()
        existing_records = {row.car_json_string for row in existing_data}
        unique_records = [record for record in records if record not in existing_records]
        
        if unique_records:
            rows_to_insert = [{'car_json_string': record} for record in unique_records]
            errors = client.insert_rows_json(table_id, rows_to_insert)
            if errors == []:
                response = True
                logger.info(f"Data successfully written to src_jiji_cars_info table: {table_id}")
            else:
                logger.error(f"Error in load_data_into_bigquery(): {str(errors)}")
        else:
            logger.info("No new unique records to insert.")
        return response
    except Exception as e:
        logger.error(f"Error in load_data_into_bigquery(): {str(e)}")
        raise

def load_bigquery_staging_table() -> None:
    try:
        client = bigquery.Client()
        query = bq.get_stg_jiji_cars_info_script()
        job = client.query(query)
        job.result()  

        if job.errors is None:
            logger.info("stg_jiji_cars_info has been refreshed successfully")
        else:
            logger.error(f"Error in load_bigquery_staging_table(): {job.errors}")
    except Exception as e:
        logger.error(f"Error in load_bigquery_staging_table(): {str(e)}")
        raise

def main() -> None:
    try:
        bucket_name = 'classified_data_scraping'
        dataset_name = 'classified_data_scraping'
        table_name = 'src_jiji_cars_info'

        records = read_files_from_bucket(bucket_name)
        response = load_bigquery_source_table(dataset_name, table_name, records)
        if response:
            load_bigquery_staging_table()
            
    except Exception as e:
        logger.error(f"Error in main(): {str(e)}")
        raise

if __name__ == '__main__':
    os.system('cls')
    main()