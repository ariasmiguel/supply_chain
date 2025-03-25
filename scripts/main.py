from clickhouse_driver import Client
import pandas as pd
import time
import sys

def wait_for_clickhouse(host='localhost', port=9000, max_retries=30, retry_interval=2):
    """Wait for ClickHouse to become available."""
    print(f"Waiting for ClickHouse to become available at {host}:{port}")
    
    for attempt in range(max_retries):
        try:
            client = Client(
                host=host,
                port=port,
                user='default',
                password=''  # Local instance doesn't need password by default
            )
            # Test the connection
            client.execute('SELECT 1')
            print("Successfully connected to ClickHouse!")
            return client
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries}: Could not connect to ClickHouse. Error: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Waiting {retry_interval} seconds before next attempt...")
                time.sleep(retry_interval)
            else:
                print("Maximum retry attempts reached. Could not connect to ClickHouse.")
                raise

def create_tables(client):
    """Create the necessary tables in ClickHouse."""
    try:
        # Create the PPI data table
        client.execute('''
            CREATE TABLE IF NOT EXISTS ppi_data
            (
                date Date,
                metric LowCardinality(String),
                value Float64,
                is_preliminary UInt8
            )
            ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, metric)
        ''')
        print("Successfully created ppi_data table")

        # Create materialized view for monthly changes
        client.execute('''
            CREATE MATERIALIZED VIEW IF NOT EXISTS ppi_monthly_changes
            (
                month Date,
                metric LowCardinality(String),
                value_avg Float64,
                mom_change Float64,
                yoy_change Float64
            )
            ENGINE = AggregatingMergeTree()
            PARTITION BY toYYYYMM(month)
            ORDER BY (month, metric)
            AS SELECT
                toStartOfMonth(date) as month,
                metric,
                avg(value) as value_avg,
                (value_avg - lagInFrame(value_avg, 1) OVER (PARTITION BY metric ORDER BY month)) / 
                    nullIf(lagInFrame(value_avg, 1) OVER (PARTITION BY metric ORDER BY month), 0) as mom_change,
                (value_avg - lagInFrame(value_avg, 12) OVER (PARTITION BY metric ORDER BY month)) / 
                    nullIf(lagInFrame(value_avg, 12) OVER (PARTITION BY metric ORDER BY month), 0) as yoy_change
            FROM ppi_data
            GROUP BY month, metric
        ''')
        print("Successfully created ppi_monthly_changes materialized view")
        
    except Exception as e:
        print(f"Error creating tables: {str(e)}")
        raise

def load_data_from_csv(client):
    """Load data from CSV file into ClickHouse."""
    try:
        # Load the CSV file
        print("Reading CSV file...")
        df = pd.read_csv('data/ppi_data_latest.csv')
        print(f"Found {len(df)} rows in CSV file")
        
        # Convert data types
        df['date'] = pd.to_datetime(df['date'])
        df['is_preliminary'] = df['is_preliminary'].astype(bool).astype(int)  # Convert to bool then int
        df['value'] = pd.to_numeric(df['value'], errors='coerce')  # Convert to float, handle any errors
        df['metric'] = df['metric'].astype(str)
        
        # Print data types for debugging
        print("\nData types in DataFrame:")
        print(df.dtypes)
        print("\nFirst few rows:")
        print(df.head())
        
        # Prepare data for insertion
        data = []
        for _, row in df.iterrows():
            try:
                date_val = row['date'].date()
                metric_val = str(row['metric'])
                value_val = float(row['value'])
                is_preliminary_val = int(row['is_preliminary'])
                
                data.append((
                    date_val,
                    metric_val,
                    value_val,
                    is_preliminary_val
                ))
            except Exception as e:
                print(f"Error converting row: {row}")
                print(f"Error details: {str(e)}")
                continue  # Skip problematic rows
        
        if not data:
            raise Exception("No valid data rows to insert")
        
        # Insert data into ClickHouse
        print("\nInserting data into ClickHouse...")
        client.execute(
            'INSERT INTO ppi_data (date, metric, value, is_preliminary) VALUES',
            data
        )
        print(f"Successfully loaded {len(data)} rows into ClickHouse")
        
        # Verify the data was inserted
        count = client.execute('SELECT count() FROM ppi_data')[0][0]
        print(f"\nTotal rows in database: {count}")
        
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        raise

def main():
    try:
        print("Starting data loading process...")
        
        # Wait for ClickHouse to be ready and get client
        client = wait_for_clickhouse()
        
        # Create tables
        print("\nCreating tables...")
        create_tables(client)
        
        # Load data
        print("\nLoading data...")
        load_data_from_csv(client)
        
        print("\nData loading process completed successfully!")
        
    except Exception as e:
        print(f"\nFatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 