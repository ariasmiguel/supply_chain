import polars as pl
import requests
from bs4 import BeautifulSoup
import pandas as pd  # Still needed for read_html as Polars doesn't have this functionality yet

def fetch_bls_data():
    """Fetch PPI data tables from BLS website and return them as Polars DataFrames."""
    url = "https://www.bls.gov/news.release/ppi.t04.htm"
    
    print(f"Fetching data from {url}")
    response = requests.get(url)
    response.raise_for_status()
    
    # Parse HTML content
    soup = BeautifulSoup(response.content, 'lxml')
    
    # Find all tables in the HTML
    tables = pd.read_html(response.text)
    
    # Convert Pandas DataFrames to Polars DataFrames
    polars_tables = [pl.from_pandas(table) for table in tables]
    
    print(f"Found {len(polars_tables)} tables")
    return polars_tables 