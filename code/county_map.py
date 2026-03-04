import pandas as pd
from pathlib import Path
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
import shapely
from matplotlib.ticker import ScalarFormatter
import matplotlib.colors as mcolors
import matplotlib.colorbar as mcolorbar

script_dir = Path(__file__).parent
output_path = script_dir / '../data/derived-data/county_map.png'

geo_data_path = script_dir / '../data/raw-data/Illinois_Counties.geojson'
il_gdf = gpd.read_file(geo_data_path)
il_gdf.head()

ghgp_data_path = script_dir / '../data/derived-data/ghgp.csv'
emissions_df = pd.read_csv(ghgp_data_path)

cancer_data_path = script_dir / '../data/derived-data/combined_cancer_by_county.csv'
cancer_df = pd.read_csv(cancer_data_path)
cancer_df['County'] = cancer_df['County'].str.lower()

cancer_gdf = il_gdf.merge(cancer_df, left_on="NAME_LC", right_on='County', how="left")

emissions_df['County'] = emissions_df['County'].str.lower()
cancer_emissions_gdf = cancer_gdf.merge(emissions_df, on='County', how="outer")


# Side by side comparison
fig, axes = plt.subplots(1, 2, figsize=(12, 8))

# Left: Cancer Incidence
inc_values = cancer_emissions_gdf["Incidence"].dropna()
bounds_left = np.unique(inc_values.quantile(np.linspace(0, 1, 6)))
if len(bounds_left) < 2:
    bounds_left = np.array([bounds_left[0], bounds_left[0] + 1])

cancer_emissions_gdf.boundary.plot(ax=axes[1], linewidth=0.2, edgecolor="black")
cancer_emissions_gdf.plot(
    ax=axes[0],
    column="Incidence",
    cmap="Blues",
    linewidth=0.2,
    edgecolor="black",
    scheme="quantiles",
    k=5,
    legend=False)

axes[0].set_title("Cancer Incidence")
axes[0].set_axis_off()

cbar_left = mcolorbar.ColorbarBase(
    axes[0].inset_axes([1.05, 0.1, 0.03, 0.8]),
    cmap=plt.cm.Blues,
    norm=mcolors.BoundaryNorm(bounds_left, plt.cm.Blues.N),
    orientation="vertical",
    ticks=np.round(bounds_left, 0),
    label="Incidence")

# Right: Total Direct Emissions
emissions_values = cancer_emissions_gdf["Total Direct Emissions"].dropna()
bounds_right = np.unique(emissions_values.quantile(np.linspace(0, 1, 6)))
if len(bounds_right) < 2:
    bounds_right = np.array([bounds_right[0], bounds_right[0] + 1])

cancer_emissions_gdf.boundary.plot(ax=axes[1], linewidth=0.2, edgecolor="black")
cancer_emissions_gdf.plot(
    ax=axes[1],
    column="Total Direct Emissions",
    cmap="Reds",
    linewidth=0.2,
    edgecolor="black",
    scheme="quantiles",
    k=5,
    legend=False)

axes[1].set_title("Total Direct Emissions")
axes[1].set_axis_off()

cbar_right = mcolorbar.ColorbarBase(
    axes[1].inset_axes([1.05, 0.1, 0.03, 0.8]),
    cmap=plt.cm.Reds,
    norm=mcolors.BoundaryNorm(bounds_right, plt.cm.Reds.N),
    orientation="vertical",
    ticks=np.round(bounds_right, 2),
    label="Total Direct Emissions")

cbar_right.ax.yaxis.set_major_formatter(ScalarFormatter())
cbar_right.ax.yaxis.get_major_formatter().set_scientific(False)
cbar_right.ax.yaxis.get_major_formatter().set_useOffset(False)

fig.suptitle("Cancer Incidence vs Total Direct Emissions",
             fontsize=16, y=0.98, x=0.58)

plt.savefig(output_path)