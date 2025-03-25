import datetime as dt
import sys
import polars as pl
import pandas as pd
import time
from clickhouse_driver import Client
from src.utils import fetch_bls_data, transform_table_to_long_format, analyze_tables
from src.utils.clickhouse import create_tables, load_data, wait_for_clickhouse

def wait_for_clickhouse(host='localhost', port=9000, max_retries=30, retry_interval=2):
    """Wait for ClickHouse to become available."""
    print(f"Waiting for ClickHouse to become available at {host}:{port}")
    
    for attempt in range(max_retries):
        try:
            client = Client(
                host=host,
                port=port,
                user='default',
                password=''
            )
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

def process_bls_data():
    """Fetch and process BLS data."""
    # Fetch data
    tables = fetch_bls_data()
    
    # First analyze the raw tables
    print("\nAnalyzing raw tables:")
    analyze_tables(tables)
    
    # Transform all tables to long format
    print("\nTransforming tables to long format...")
    all_data = []
    
    # Table 1: Final demand data
    print("\nProcessing Table 1 (Final demand)...")
    table_1 = tables[0]
    final_demand_data = transform_table_to_long_format(table_1)
    all_data.append(final_demand_data)
    
    # Table 2: Processed goods data
    print("\nProcessing Table 2 (Processed goods)...")
    table_2 = tables[1]
    processed_goods_data = transform_table_to_long_format(table_2)
    all_data.append(processed_goods_data)
    
    # Table 3: Services data
    print("\nProcessing Table 3 (Services)...")
    table_3 = tables[2]
    services_data = transform_table_to_long_format(table_3)
    all_data.append(services_data)
    
    # Table 4: Stage data
    print("\nProcessing Table 4 (Stage data)...")
    table_4 = tables[3]
    stage_data = transform_table_to_long_format(table_4)
    all_data.append(stage_data)
    
    # Combine all data
    print("\nCombining all data...")
    long_format_data = pl.concat(all_data)
    
    # Display the results
    print("\nFinal transformed data:")
    print("-" * 50)
    print(f"Shape: {long_format_data.shape}")
    print("\nFirst few rows:")
    print(long_format_data.head(10))
    print("\nData Types:")
    print(long_format_data.schema)
    
    # Display some summary statistics
    print("\nSummary:")
    date_range = long_format_data.select([
        pl.col('date').min(),
        pl.col('date').max()
    ])
    print(f"Date range: {date_range[0][0]} to {date_range[0][1]}")
    
    unique_metrics = long_format_data.select(pl.col('metric')).unique()
    print(f"Number of unique metrics: {len(unique_metrics)}")
    print(f"Metrics: {sorted(unique_metrics.to_series().to_list())}")
    
    prelim_count = long_format_data.select(pl.col('is_preliminary')).sum()
    print(f"Number of preliminary values: {int(prelim_count[0][0])}")
    
    # Save to CSV for inspection
    print("\nSaving processed data to CSV...")
    long_format_data.write_csv('data/ppi_data_latest.csv')
    print("Data saved to data/ppi_data_latest.csv")
    
    return long_format_data

def main():
    try:
        # Step 1: Process BLS data
        print("Step 1: Processing BLS data...")
        df = process_bls_data()
        
        # Step 2: Wait for ClickHouse to be ready
        client = wait_for_clickhouse()
        
        # Step 3: Create tables in ClickHouse
        print("\nStep 3: Creating tables in ClickHouse...")
        create_tables(client)
        
        # Step 4: Load data to ClickHouse
        print("\nStep 4: Loading data to ClickHouse...")
        load_data(client, df)
        
        print("\nAll processes completed successfully!")
        
        # Step 5: Verify specific value
        result = client.execute('''
            SELECT date, metric, value, is_preliminary 
            FROM ppi_data 
            WHERE metric = 'change_in_final_demand_from_12_months_ago' 
            AND date = '2024-02-01'
        ''')
        print("\nVerifying specific value:")
        print(f"Date: {result[0][0]}")
        print(f"Metric: {result[0][1]}")
        print(f"Value: {result[0][2]}")
        print(f"Is Preliminary: {result[0][3]}")
        
    except Exception as e:
        print(f"\nFatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()


