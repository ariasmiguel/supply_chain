import os
import sys
from src.utils.clickhouse import (
    wait_for_clickhouse,
    get_stage_summary,
    get_price_transmission,
    export_all_data
)
from src.utils.visualization import (
    create_supply_chain_pressure_chart,
    create_price_transmission_chart,
    save_charts
)

def main():
    try:
        # Create output directory if it doesn't exist
        os.makedirs('output', exist_ok=True)
        
        print("Connecting to ClickHouse...")
        client = wait_for_clickhouse()
        
        # Export all data to CSV
        print("\nExporting all data to CSV...")
        export_all_data(client)
        
        # Generate supply chain pressure visualization
        print("\nGenerating supply chain pressure visualization...")
        stage_data = get_stage_summary(client)
        print("\nStage data:", stage_data)
        print("\nStage data types:")
        for stage, data in stage_data.items():
            print(f"\n{stage}:")
            for key, value in data.items():
                print(f"  {key}: {type(value)} = {value}")
                if isinstance(value, float):
                    print(f"    Converting {value} to string: {str(value)}")
        
        print("\nCreating pressure chart...")
        pressure_fig = create_supply_chain_pressure_chart(stage_data)
        
        # Generate price transmission visualizations (to be implemented)
        # print("\nGenerating price transmission visualizations...")
        # corn_data = get_price_transmission(client, 'corn')
        # auto_parts_data = get_price_transmission(client, 'auto_parts')
        # transmission_fig = create_price_transmission_chart(...)
        
        # Save visualizations
        print("\nSaving visualizations...")
        save_charts(pressure_fig)
        
        print("\nVisualizations have been generated successfully!")
        print("Files saved in the output/ directory:")
        print("- ppi_data_export.csv (complete dataset)")
        print("- supply_chain_pressure.html (interactive version)")
        print("- supply_chain_pressure.png (static version)")
        
    except Exception as e:
        print(f"\nError generating visualizations: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 