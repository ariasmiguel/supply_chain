import datetime as dt
import sys
import polars as pl
from src.utils import fetch_bls_data, transform_table_to_long_format, analyze_tables
from src.utils.clickhouse import wait_for_clickhouse, create_tables, load_data

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
        pl.col('date').min().alias('min_date'),
        pl.col('date').max().alias('max_date')
    ]).collect()
    print(f"Date range: {date_range[0]['min_date']} to {date_range[0]['max_date']}")
    
    unique_metrics = long_format_data.select(pl.col('metric')).unique()
    print(f"Number of unique metrics: {len(unique_metrics)}")
    print(f"Metrics: {sorted(unique_metrics.to_series().to_list())}")
    
    prelim_count = long_format_data.select(pl.col('is_preliminary').sum()).collect()[0][0]
    print(f"Number of preliminary values: {int(prelim_count)}")
    
    return long_format_data

def load_to_clickhouse(df):
    """Load data into ClickHouse."""
    print("\nStarting ClickHouse data loading process...")
    
    # Wait for ClickHouse to be ready and get client
    client = wait_for_clickhouse()
    
    # Create tables
    print("\nCreating tables...")
    create_tables(client)
    
    # Load data
    print("\nLoading data...")
    load_data(client, df)
    
    print("\nData loading process completed successfully!")

def main():
    try:
        # Step 1: Process BLS data
        print("Step 1: Processing BLS data...")
        long_format_data = process_bls_data()
        
        # Step 2: Load to ClickHouse
        print("\nStep 2: Loading data to ClickHouse...")
        load_to_clickhouse(long_format_data)
        
        print("\nAll processes completed successfully!")
        
    except Exception as e:
        print(f"\nFatal error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()


