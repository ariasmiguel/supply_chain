import polars as pl
import requests
from bs4 import BeautifulSoup
import pandas as pd  # Still needed for read_html as Polars doesn't have this functionality yet

def fetch_bls_data():
    """Fetch PPI data tables from BLS website and return them as Polars DataFrames."""
    url = "https://www.bls.gov/news.release/ppi.t04.htm"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1'
    }
    
    print(f"Fetching data from {url}")
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    
    # Parse HTML content
    soup = BeautifulSoup(response.content, 'lxml')
    
    # Find all tables in the HTML
    tables = pd.read_html(response.text)
    
    # Convert Pandas DataFrames to Polars DataFrames
    polars_tables = [pl.from_pandas(table) for table in tables]
    
    print(f"Found {len(polars_tables)} tables")
    return polars_tables 