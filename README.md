# Supply Chain Price Pressure Analysis

This project analyzes supply chain price pressures using Bureau of Labor Statistics (BLS) Producer Price Index (PPI) data. It processes data through different stages of the supply chain and visualizes price pressures and transmission effects.

## Features

- Data fetching from BLS website
- Data processing and transformation using Polars
- ClickHouse database integration for efficient data storage and querying
- Interactive visualizations using Plotly
- Supply chain pressure analysis across production stages
- Price transmission analysis through the supply chain

## Requirements

- Python 3.8+
- ClickHouse server
- Dependencies listed in requirements.txt

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd supply_chain
```

2. Create and activate a virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Start ClickHouse server:
```bash
# Follow ClickHouse installation instructions for your OS
# Ensure ClickHouse is running on localhost:9000
```

## Usage

1. Run the main script to fetch and process data:
```bash
python scripts/main.py
```

2. Generate visualizations:
```bash
python scripts/generate_visualizations.py
```

The script will:
- Export all data to CSV for exploratory analysis
- Generate an interactive supply chain pressure visualization
- Save visualizations in HTML and PNG formats

## Project Structure

```
supply_chain/
├── src/
│   └── utils/
│       ├── clickhouse/     # ClickHouse database utilities
│       ├── data_fetcher.py # BLS data fetching
│       └── visualization.py # Visualization utilities
├── scripts/
│   ├── main.py            # Main data processing script
│   └── generate_visualizations.py # Visualization generation
├── output/                # Generated visualizations and data
├── requirements.txt       # Project dependencies
└── README.md             # This file
```

## Data Sources

The project uses Producer Price Index (PPI) data from the Bureau of Labor Statistics (BLS), focusing on:
- Stage 1-4 intermediate demand
- Price transmission through supply chains
- Monthly and yearly price changes

## Visualizations

The project generates two main types of visualizations:
1. Supply Chain Pressure Chart: Shows current pressure levels across production stages
2. Price Transmission Chart: Visualizes how price changes propagate through the supply chain

## Contributing

Feel free to open issues or submit pull requests with improvements.

## License

MIT License - see LICENSE file for details 