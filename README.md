# Supply Chain Pressure Analysis Tool

A Python-based tool for analyzing and visualizing supply chain pressure using Producer Price Index (PPI) data. The tool processes PPI data through various stages of production to identify pressure points and trends in the supply chain.

## Features

- Data fetching from official PPI sources
- Efficient data processing using Polars DataFrame library
- ClickHouse database integration for high-performance data storage and querying
- Interactive visualizations of supply chain pressure metrics
- Analysis of price transmission across production stages

## Requirements

- Python 3.8+
- ClickHouse database
- Docker and Docker Compose (for running ClickHouse)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/supply_chain.git
cd supply_chain
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Start ClickHouse using Docker Compose:
```bash
docker-compose up -d
```

## Usage

1. Load PPI data into ClickHouse:
```bash
python scripts/main.py
```

2. Generate visualizations:
```bash
python scripts/generate_visualizations.py
```

The visualizations will be saved in the `output` directory.

## Project Structure

```
supply_chain/
├── config/                 # ClickHouse configuration files
├── docker-compose.yml     # Docker Compose configuration
├── requirements.txt       # Python dependencies
├── scripts/              # Main execution scripts
│   ├── main.py           # Data loading script
│   └── generate_visualizations.py  # Visualization generation script
└── src/                  # Source code
    └── utils/            # Utility modules
        ├── data_fetcher.py       # Data fetching utilities
        ├── data_transformer.py   # Data transformation utilities
        └── visualization.py      # Visualization utilities
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details. 