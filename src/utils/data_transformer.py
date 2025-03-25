import polars as pl
import re

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
    
    # Extract year rows
    year_rows = df.filter(pl.col('month').str.contains(r'^\d{4}$'))
    years = year_rows.select('month').to_series().to_list()
    print("\nDebug: Year rows found:", years)
    
    # Remove year and footnote rows
    df = df.filter(~pl.col('month').str.contains(r'^\d{4}$|^Footnotes'))
    
    print("\nDebug: Month parsing")
    # Clean month names and create temporary date string
    month_map = {
        'Jan.': 'Jan', 'Feb.': 'Feb', 'Mar.': 'Mar', 'Apr.': 'Apr',
        'May': 'May', 'June': 'Jun', 'July': 'Jul', 'Aug.': 'Aug',
        'Sept.': 'Sep', 'Oct.': 'Oct', 'Nov.': 'Nov', 'Dec.': 'Dec'
    }
    
    # Create a reference year for month parsing
    df = df.with_columns([
        pl.col('month').str.extract(r'^([A-Za-z]+\.?)', group=1).alias('month_clean'),
        pl.lit('2000').alias('ref_year')
    ])
    
    # Map month abbreviations
    df = df.with_columns(
        pl.col('month_clean').map_dict(month_map, default=pl.col('month_clean')).alias('month_clean')
    )
    
    # Create temporary date string for parsing
    df = df.with_columns(
        pl.concat_str([pl.col('ref_year'), pl.lit(' '), pl.col('month_clean')]).alias('temp_date')
    )
    
    print("Original and cleaned months:")
    print(df.select(['month', 'temp_date']))
    
    # Parse month numbers
    df = df.with_columns(
        pl.col('temp_date').str.strptime(pl.Date, fmt='%Y %b').dt.month().alias('month_num')
    )
    
    print("\nFinal month numbers:")
    print(df)
    
    # Remove rows where month_num is null
    df = df.drop_nulls('month_num')
    print("\nDebug: Removed rows with NULL dates")
    print(f"Remaining rows: {len(df)}")
    
    # Create proper dates using the year and month
    current_year = years[0]  # Assuming first year is current
    next_year = str(int(current_year) + 1)
    
    df = df.with_columns([
        pl.when(pl.col('month_num') >= 1)
        .then(pl.concat_str([
            pl.lit(current_year if pl.col('month_num') >= 2 else next_year),
            pl.lit('-'),
            pl.col('month_num').cast(str).str.zfill(2),
            pl.lit('-01')
        ]))
        .alias('date')
    ])
    
    # Convert date string to date type
    df = df.with_columns(pl.col('date').str.strptime(pl.Date, fmt='%Y-%m-%d'))
    
    # Drop utility columns
    df = df.drop(['month', 'month_clean', 'ref_year', 'temp_date', 'month_num'])
    
    # Melt the dataframe
    id_vars = ['date']
    value_vars = [col for col in df.columns if col not in id_vars]
    
    melted = df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        variable_name='metric',
        value_name='value'
    )
    
    # Convert value column to float
    melted = melted.with_columns(pl.col('value').cast(pl.Float64))
    
    # Add is_preliminary column
    melted = melted.with_columns(
        pl.col('value').cast(str).str.contains(r'\(1\)|\(P\)').alias('is_preliminary')
    )
    
    # Clean value column by removing markers
    melted = melted.with_columns(
        pl.col('value').cast(str).str.extract(r'^([0-9.-]+)').cast(pl.Float64)
    )
    
    print("\nDebug: Final melted data structure")
    print(melted.head(10))
    
    print("\nUnique metrics after normalization:")
    print(sorted(melted.select('metric').unique().to_series().to_list()))
    
    return melted

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