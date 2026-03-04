# Analysis of Carbon Emissions and Public Health Outcomes

This project processes and visualizes carbon emissions by county in Illinois and tracks their relationship to public health outcomes across the state, including cancer, COPD, asthma, and worsening complications for Covid-19, stroke, and heart failure.

## Setup

```bash
conda env create -f environment.yml
```

## Project Structure

```
data/
  raw-data/           # Raw data files
    fire.csv          # Historical fire perimeter data
    canadian_cpi.csv  # Canadian Consumer Price Index data
  derived-data/       # Filtered data and output plots
    fire_filtered.gpkg  # Fire data filtered to post-2015
    cpi_filtered.csv    # CPI data filtered to 2020 onwards
code/                 # Data processing and plot creation 
  preprocessing.py    # Filters fire and CPI data
  plot_fires.py       # Plots fire perimeters
```

## Usage

1. Run preprocessing to filter data:
   ```bash
   python code/preprocessing.py
   ```

2. Generate the fire perimeter plot:
   ```bash
   python code/plot_fires.py
   ```
