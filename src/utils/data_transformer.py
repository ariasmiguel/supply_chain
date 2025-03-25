import polars as pl
import re
from datetime import datetime

def transform_table_to_long_format(table):
    """Transform a BLS table from wide to long format using Polars."""
    print("\nDebug: Initial table structure")
    print("Columns:", table.columns)
    print("First few rows:")
    print(table.head())
    print("\nColumn types:")
    print(table.dtypes)
    
    # Normalize column names
    normalized_cols = {}
    for col in table.columns:
        if isinstance(col, tuple):
            # Join tuple elements and clean
            col_name = '_'.join(col).lower()
        else:
            col_name = col.lower()
        
        # Replace spaces and special characters with underscores
        col_name = re.sub(r'[^a-z0-9]+', '_', col_name)
        # Remove leading/trailing underscores
        col_name = col_name.strip('_')
        normalized_cols[col] = col_name
    
    # Rename columns
    df = table.rename(normalized_cols)
    
    print("\nDebug: Normalized columns:")
    print(df.columns)
    
    # Extract the grouping column which contains the metric names
    df = df.with_columns(
        pl.col('grouping_grouping').str.replace_all(r'\s+', ' ').alias('metric')
    )
    
    # Get the current date from the column names
    # Example: ('Unadjusted index', 'Oct. 2021(2)')
    date_cols = [col for col in table.columns if isinstance(col, tuple) and 'Unadjusted index' in col[0]]
    if not date_cols:
        raise ValueError("Could not find date columns in the table")
    
    # Get the most recent date column
    latest_date_col = date_cols[-1]
    date_str = latest_date_col[1].split('(')[0].strip()  # Remove any (2) or similar markers
    
    # Parse the date
    try:
        date = datetime.strptime(date_str, '%b. %Y')
    except ValueError:
        try:
            date = datetime.strptime(date_str, '%B %Y')
        except ValueError:
            raise ValueError(f"Could not parse date string: {date_str}")
    
    # Create a date column
    df = df.with_columns(
        pl.lit(date.strftime('%Y-%m-%d')).alias('date')
    )
    
    # Get the value from the latest month's column
    # Use the unadjusted index for the latest month
    df = df.with_columns(
        pl.col(latest_date_col).alias('value')
    )
    
    # Convert value to float and handle any special markers
    df = df.with_columns([
        # Check for preliminary values (marked with (P) or similar)
        pl.col('value').cast(str).str.contains(r'\([P12]\)').alias('is_preliminary'),
        # Extract the numeric value
        pl.col('value').cast(str).str.extract(r'^([0-9.-]+)').cast(pl.Float64).alias('value')
    ])
    
    # Select only the columns we need
    df = df.select(['date', 'metric', 'value', 'is_preliminary'])
    
    # Remove rows with null values
    df = df.drop_nulls()
    
    print("\nDebug: Final data structure")
    print(df.head(10))
    
    print("\nUnique metrics after normalization:")
    print(sorted(df.select('metric').unique().to_series().to_list()))
    
    return df

def analyze_tables(tables):
    """Analyze the structure of BLS tables using Polars."""
    for i, table in enumerate(tables, 1):
        print(f"\nTable {i}:")
        print("-" * 50)
        print(f"Shape: {table.shape}")
        print(f"Columns: {table.columns}")
        print("\nFirst few rows:")
        print(table.head())
        print("\nData Types:")
        print(table.dtypes) 