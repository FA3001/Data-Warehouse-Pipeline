import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from urllib.parse import quote

# Configure logging
logging.basicConfig(filename='data_pipeline.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

class PostgreSQLManager:
    def __init__(self, username, password, host, port, database):
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = None
        self.connect()

    def create_connection_string(self):
        encoded_password = quote(self.password)
        return f'postgresql://{self.username}:{encoded_password}@{self.host}:{self.port}/{self.database}'

    def connect(self):
        try:
            DATABASE_URI = self.create_connection_string()
            self.engine = create_engine(DATABASE_URI, pool_size=10, max_overflow=20, pool_timeout=30)
            connection = self.engine.connect()
            logging.info("Connected to PostgreSQL database.")
            connection.close()
            return True
        except OperationalError as e:
            logging.error(f"Failed to connect to database: {e}")
            return False

    def upload_data(self, df, table_name, if_exists='replace'):
        if self.engine is None:
            logging.error("No database connection. Call connect() first.")
            return False
        try:
            # Basic validation before upload
            if df.empty:
                logging.warning(f"Empty DataFrame for table '{table_name}'.")
                return False
            df.to_sql(table_name, self.engine, index=False, if_exists=if_exists, method='multi')
            logging.info(f"Uploaded {len(df)} rows to '{table_name}'.")
            return True
        except Exception as e:
            logging.error(f"Failed to upload to '{table_name}': {e}")
            return False
