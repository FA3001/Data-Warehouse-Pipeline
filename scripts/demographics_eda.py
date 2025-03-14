import duckdb
import pandas as pd
from postgres_manager import PostgreSQLManager
import logging

class Demographics(PostgreSQLManager):
    def __init__(self, file_path, username, password, host, port, database):
        super().__init__(username, password, host, port, database)
        self.file_path = file_path
        self.cursor = duckdb.connect("")
        logging.info(f"Initialized Demographics with file_path: {self.file_path}")

    def convert_json_to_parquet(self, json_file_path, parquet_file_path):
        """
        Convert a JSON file to a Parquet file using DuckDB.
        """
        logging.info(f"Starting JSON to Parquet conversion: {json_file_path} -> {parquet_file_path}")
        try:
            self.cursor.execute(f"""
                CREATE TABLE demographics AS
                SELECT *
                FROM read_json_auto('{json_file_path}');
            """)
            logging.debug("Loaded JSON data into DuckDB table.")

            self.cursor.execute(f"""
                COPY demographics TO '{parquet_file_path}' (FORMAT 'parquet');
            """)
            logging.info(f"Successfully converted '{json_file_path}' to '{parquet_file_path}'.")

            json_schema = self.cursor.execute(f"""
                DESCRIBE SELECT * FROM read_json_auto('{json_file_path}');
            """).fetchall()
            parquet_schema = self.cursor.execute(f"""
                DESCRIBE SELECT * FROM read_parquet('{parquet_file_path}');
            """).fetchall()

            if json_schema == parquet_schema:
                logging.info("✅ Schema unchanged after conversion.")
            else:
                logging.warning("❌ Schema changed after conversion. Differences detected.")

        except Exception as e:
            logging.error(f"Error during JSON to Parquet conversion: {e}")
            raise
        finally:
            self.cursor.execute("DROP TABLE IF EXISTS demographics;")
            logging.debug("Cleaned up temporary DuckDB table.")

    def build_select_clause(self, fields, years):
        """Build SQL SELECT clause for querying nested fields and years."""
        logging.debug(f"Building SELECT clause for fields: {fields}, years: {years}")
        select_clause = ["state", "county"]
        for field in fields:
            parts = field.split(".")
            base_col = parts[0]
            nested_path = parts[1:]
            for year in years:
                access_str = f'"{base_col}"'
                for part in nested_path:
                    access_str += f'."{part}"'
                access_str += f'."{year}"'
                alias = f"{field}.{year}"
                select_clause.append(f"{access_str} AS \"{alias}\"")
        logging.debug(f"Generated SELECT clause: {', '.join(select_clause)}")
        return select_clause

    def query_data(self, fields, years):
        """Query data from the Parquet file based on specified fields and years."""
        logging.info(f"Querying data from Parquet: {self.file_path} for fields: {fields}, years: {years}")
        if not fields or not years:
            logging.error("Fields or years are empty. Cannot query data.")
            raise ValueError("Fields and years must not be empty.")
        
        select_clause = self.build_select_clause(fields, years)
        query = f"""
            SELECT 
                {', '.join(select_clause)}
            FROM read_parquet('{self.file_path}');
        """
        try:
            df = self.cursor.execute(query).df()
            logging.info(f"Successfully queried data. DataFrame shape: {df.shape}")
            if df.empty:
                logging.warning("Query returned an empty DataFrame.")
            return df
        except Exception as e:
            logging.error(f"Error querying Parquet data: {e}")
            return None

    def validate_data(self, df):
        """Validate demographics data."""
        logging.info("Validating demographics data...")
        if df is None:
            logging.error("Input DataFrame is None.")
            raise ValueError("DataFrame is None.")
        if df.empty:
            logging.warning("DataFrame is empty.")
            return  # Allow empty DataFrame but log it
        if 'state' not in df.columns or 'county' not in df.columns:
            logging.error("Missing 'state' or 'county' columns.")
            raise ValueError("Missing required columns: 'state' or 'county'.")
        numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
        for col in numeric_cols:
            if df[col].min() < 0:
                logging.warning(f"Negative values found in '{col}'.")

    def eda_demo(self, df_demo):
        """Perform EDA and transform demographics data."""
        logging.info("Starting EDA on demographics data...")
        if df_demo is None:
            logging.error("Input DataFrame is None.")
            raise ValueError("The input DataFrame is None. Check the query_data method.")
        if df_demo.empty:
            logging.warning("Input DataFrame is empty. Skipping EDA.")
            return pd.DataFrame()  # Return empty DataFrame gracefully

        self.validate_data(df_demo)
        logging.debug(f"Initial DataFrame shape: {df_demo.shape}")

        # Step 1: Filter out columns containing 'census'
        filtered_columns = [col for col in df_demo.columns if 'census' not in col.lower()]
        df_demo = df_demo[filtered_columns]
        logging.debug(f"Filtered columns (no 'census'): {filtered_columns}")

        # Step 2: Melt the DataFrame
        melted_df = pd.melt(df_demo, id_vars=['state', 'county'], var_name='column_name', value_name='value')
        logging.debug(f"Melted DataFrame shape: {melted_df.shape}")

        # Step 3: Extract year and category
        melted_df[['category', 'year']] = melted_df['column_name'].str.rsplit('.', n=1, expand=True)
        melted_df = melted_df.drop(columns='column_name')
        logging.debug(f"Extracted category and year. Columns: {melted_df.columns.tolist()}")

        # Step 4: Rename and clean categories
        melted_df['category'] = melted_df['category'].replace({
            'unemployment.employed': 'population.unemployment.employed',
            'unemployment.unemployed': 'population.unemployment.unemployed'
        })
        melted_df['category'] = melted_df['category'].str.replace('population.', '', regex=True).str.replace('by_', '', regex=True)
        logging.debug(f"Cleaned categories: {melted_df['category'].unique()}")

        # Step 5: Sort and filter
        melted_df['year'] = melted_df['year'].astype(int)
        melted_df = melted_df[(melted_df['year'] >= 2011) & (melted_df['year'] < 2020)]
        melted_df.sort_values(['state', 'county', 'year'], inplace=True)
        logging.debug(f"Filtered and sorted DataFrame shape: {melted_df.shape}")

        # Step 6: Pivot the DataFrame
        pivoted_df = melted_df.pivot_table(index=['state', 'county', 'year'], columns='category', values='value', aggfunc='first')
        pivoted_df.reset_index(inplace=True)
        logging.debug(f"Pivoted DataFrame shape: {pivoted_df.shape}")

        # Step 7: Handle missing values
        numeric_columns = pivoted_df.select_dtypes(include=['number']).columns
        for column in numeric_columns:
            median_value = pivoted_df.groupby('county')[column].transform('median').fillna(0).astype('int64')
            pivoted_df[column] = pivoted_df[column].fillna(median_value)
            logging.debug(f"Filled NaN in '{column}' with median values.")

        # Step 8: Drop the 'county' column
        pivoted_df = pivoted_df.drop(columns='county', axis=1)
        logging.info(f"Completed EDA. Final DataFrame shape: {pivoted_df.shape}")

        return pivoted_df

    def upload_to_postgres(self, fields, years, table_name='demographics', incremental=False):
        """Upload cleaned data to PostgreSQL."""
        logging.info(f"Uploading data to PostgreSQL table '{table_name}' (incremental={incremental})")
        df_cleaned = self.eda_demo(self.query_data(fields, years))
        if df_cleaned is None:
            logging.error("Cleaned DataFrame is None. Aborting upload.")
            return
        if df_cleaned.empty:
            logging.warning(f"Cleaned DataFrame is empty. No data to upload to '{table_name}'.")
            return

        try:
            if incremental:
                existing = pd.read_sql(f"SELECT DISTINCT state, year FROM {table_name}", self.engine)
                logging.debug(f"Fetched {len(existing)} existing state-year combinations from '{table_name}'.")
                df_cleaned = df_cleaned[~df_cleaned[['state', 'year']].apply(tuple, axis=1).isin(existing.apply(tuple, axis=1))]
                logging.debug(f"After incremental filter, DataFrame shape: {df_cleaned.shape}")

            if not df_cleaned.empty:
                self.upload_data(df_cleaned, table_name, if_exists='append' if incremental else 'replace')
                logging.info(f"Successfully uploaded {len(df_cleaned)} rows to '{table_name}'.")
            else:
                logging.info(f"No new data to upload to '{table_name}' after incremental check.")
        except Exception as e:
            logging.error(f"Error uploading data to '{table_name}': {e}")
            raise