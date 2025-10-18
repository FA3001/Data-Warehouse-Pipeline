import duckdb
import pandas as pd
from postgres_manager import PostgreSQLManager
import logging

class ComplaintsEDA(PostgreSQLManager):
    def __init__(self, file_path, username, password, host, port, database):
        super().__init__(username, password, host, port, database)
        self.file_path = file_path
        self.conn = duckdb.connect("")

    def validate_data(self, df):
        """Validate raw data before processing."""
        logging.info("Starting data validation...")
        # Schema check
        expected_cols = {'Date received', 'Product', 'State'}
        missing_cols = expected_cols - set(df.columns)
        if missing_cols:
            logging.error(f"Missing columns: {missing_cols}")
            raise ValueError(f"Missing columns: {missing_cols}")
        
        # Missing data check
        missing_pct = df.isnull().mean() * 100
        for col, pct in missing_pct.items():
            if pct > 10:
                logging.warning(f"Column '{col}' has {pct:.2f}% missing values.")
        
        # Range check
        df['Date received'] = pd.to_datetime(df['Date received'], errors='coerce')
        if df['Date received'].min() < pd.Timestamp('2010-01-01'):
            logging.warning("Dates before 2010 detected.")

    def load_data(self):
        query = f"CREATE TABLE complaints AS SELECT * FROM read_csv_auto('{self.file_path}');"
        self.conn.execute(query)
        df = self.conn.execute("SELECT * FROM complaints").df()
        self.validate_data(df)

    def clean_data(self):
        # Existing cleaning steps...
        self.conn.execute("""
            ALTER TABLE complaints DROP COLUMN Tags;
            ALTER TABLE complaints DROP COLUMN "ZIP code";
            ALTER TABLE complaints ALTER COLUMN "Date received" TYPE DATE USING "Date received"::DATE;
        """)
        # Add duplicate check
        self.conn.execute("""
            DELETE FROM complaints WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM complaints GROUP BY "Date received", "Product", "State"
            );
        """)
        logging.info("Cleaning completed with duplicate removal.")

    def upload_to_postgres(self, table_name='complaints', incremental=False):
        chunk_size = 100000
        offset = 0
        while True:
            query = f"SELECT * FROM complaints LIMIT {chunk_size} OFFSET {offset};"
            df_chunk = self.conn.execute(query).df()
            if df_chunk.empty:
                break
            if incremental:
                # Check for existing data based on a unique key
                existing_keys = pd.read_sql(f"SELECT DISTINCT \"Date received\" FROM {table_name}", self.engine)
                df_chunk = df_chunk[~df_chunk['Date received'].isin(existing_keys['Date received'])]
            self.upload_data(df_chunk, table_name, if_exists='append' if offset > 0 or incremental else 'replace')
            offset += chunk_size
