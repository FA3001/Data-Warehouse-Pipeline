import os
import logging
from demographics_eda import Demographics
from complaints_eda import ComplaintsEDA 
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../scripts'))
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    try:
        logging.info("Starting demographics pipeline...")

        # Load from .env or config
        username = os.getenv('PG_USERNAME', 'admin')
        password = os.getenv('PG_PASSWORD', 'admin')
        host = os.getenv('PG_HOST', '172.18.0.2')
        port = os.getenv('PG_PORT', '5432')
        database = os.getenv('PG_DATABASE', 'admin')


        # Complaints pipeline
        complaints_eda = ComplaintsEDA(
            file_path=os.getenv('COMPLAINTS_PATH', '/workspaces/Data-Warehouse-Pipeline/complaints.csv'),
            username=username, password=password, host=host, port=port, database=database
        )
        logging.info("Load COMPLAINTS data...")
        complaints_eda.load_data()
        logging.info("COMPLAINTS data to PostgreSQL...")
        complaints_eda.clean_data()
        logging.info("Uploading COMPLAINTS data to PostgreSQL...")
        complaints_eda.upload_to_postgres(table_name='complaints', incremental=True)


        # Load file paths from environment variables
        json_file_path = os.getenv('JSON_PATH', 'us_county_demographics.json')
        parquet_file_path = os.getenv('PARQUET_PATH', 'us_county_demographics.parquet')

        # Initialize Demographics class
        demographics = Demographics(
            file_path=parquet_file_path,
            username=username, password=password, host=host, port=port, database=database
        )

        # Define fields and years
        fields = ["unemployment.employed", "unemployment.unemployed", "population_by_age.total.18_over", "population_by_age.total.65_over"]
        years = range(2011, 2020)

        # Convert JSON to Parquet
        logging.info("Converting JSON to Parquet...")
        demographics.convert_json_to_parquet(
            json_file_path=json_file_path,
            parquet_file_path=parquet_file_path
        )

        # Upload to PostgreSQL
        logging.info("Uploading data to PostgreSQL...")
        demographics.upload_to_postgres(fields, years, table_name='demographics', incremental=True)

        logging.info("Demographics pipeline completed successfully.")

    except Exception as e:
        logging.error(f"Error in demographics pipeline: {e}")
        raise

if __name__ == "__main__":
    main()
