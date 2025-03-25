import plotly.graph_objects as go
from plotly.subplots import make_subplots
import polars as pl
from typing import Dict
import os

# Colorblind-friendly color palette
COLORS = {
    'Stage 1': '#1A365D',  # Navy blue
    'Stage 2': '#2A9D8F',  # Teal
    'Stage 3': '#E9C46A',  # Amber
    'Stage 4': '#6B4E71',  # Deep Purple
    'pressure': {
        'High': '#9B2226',    # Dark red
        'Medium': '#EE9B00',  # Orange
        'Low': '#005F73'      # Dark teal
    }
}

def create_supply_chain_pressure_chart(stage_data: Dict) -> go.Figure:
    """
    Create a modern, colorblind-friendly supply chain pressure visualization
    """
    print("\nDebug - Stage data:", stage_data)
    
    fig = make_subplots(
        rows=1, cols=4,
        subplot_titles=[f"Stage {i}" for i in range(1, 5)],
        horizontal_spacing=0.05
    )

    for idx, (stage, data) in enumerate(stage_data.items(), 1):
        print(f"\nDebug - Processing {stage}:", data)
        print(f"Debug - Data types: monthly_change={type(data['monthly_change'])}, yearly_change={type(data['yearly_change'])}, pressure_level={type(data['pressure_level'])}")
        
        # Create the main stage box
        fig.add_trace(
            go.Scatter(
                x=[0, 1, 1, 0, 0],
                y=[0, 0, 1, 1, 0],
                fill="toself",
                fillcolor=COLORS[stage],
                line=dict(color=COLORS[stage]),
                showlegend=False,
                hoverinfo='skip',
                opacity=0.2
            ),
            row=1, col=idx
        )

        # Add stage title and description
        fig.add_annotation(
            x=0.5, y=0.9,
            text=f"<b>{stage}</b>",
            showarrow=False,
            font=dict(size=14, color='black'),
            row=1, col=idx
        )

        # Add yearly change
        yearly_change = float(data['yearly_change'])  # Ensure float type
        fig.add_annotation(
            x=0.5, y=0.7,
            text=f"12-Month Change<br><b>{yearly_change:.1f}%</b>",
            showarrow=False,
            font=dict(size=12),
            row=1, col=idx
        )

        # Add current value
        current_value = float(data['monthly_change'])  # Ensure float type
        fig.add_annotation(
            x=0.5, y=0.5,
            text=f"Current Value<br><b>{current_value:.1f}</b>",
            showarrow=False,
            font=dict(size=12),
            row=1, col=idx
        )

        # Add pressure indicator
        pressure_level = str(data['pressure_level'])  # Ensure string type
        pressure_color = COLORS['pressure'][pressure_level]
        fig.add_trace(
            go.Scatter(
                x=[0.3, 0.7],
                y=[0.2, 0.2],
                fill='toself',
                fillcolor=pressure_color,
                line=dict(color=pressure_color),
                showlegend=False,
                hovertext=f"Pressure Level: {pressure_level}",
                hoverinfo='text'
            ),
            row=1, col=idx
        )

        # Add pressure level text
        fig.add_annotation(
            x=0.5, y=0.3,
            text=f"<b>{pressure_level}</b>",
            showarrow=False,
            font=dict(size=12, color=pressure_color),
            row=1, col=idx
        )

    # Update layout
    fig.update_layout(
        title=dict(
            text="Supply Chain Price Pressures Across Production Stages",
            x=0.5,
            y=0.95,
            xanchor='center',
            yanchor='top',
            font=dict(size=20)
        ),
        showlegend=False,
        height=500,
        paper_bgcolor='white',
        plot_bgcolor='white',
        margin=dict(t=100, l=50, r=50, b=50)
    )

    # Remove axes
    fig.update_xaxes(showgrid=False, zeroline=False, visible=False)
    fig.update_yaxes(showgrid=False, zeroline=False, visible=False)

    return fig

def create_price_transmission_chart(transmission_data: pl.DataFrame, title: str) -> go.Figure:
    """
    Create a modern, colorblind-friendly price transmission visualization
    """
    # Implementation for price transmission visualization
    # This will be similar to the pressure chart but with a different layout
    # showing the flow of price changes through the supply chain
    pass  # We can implement this next

def save_charts(pressure_fig: go.Figure, transmission_fig: go.Figure = None):
    """
    Save the charts as HTML and PNG files
    """
    # Create output directory if it doesn't exist
    os.makedirs("output", exist_ok=True)
    
    pressure_fig.write_html("output/supply_chain_pressure.html")
    pressure_fig.write_image("output/supply_chain_pressure.png", scale=2)
    
    if transmission_fig:
        transmission_fig.write_html("output/price_transmission.html")
        transmission_fig.write_image("output/price_transmission.png", scale=2) 