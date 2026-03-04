import pandas as pd
import altair as alt 
from pathlib import Path
import numpy as np

script_dir = Path(__file__).parent
data_path = script_dir / '../data/derived-data/combined_cancer_by_county.csv'
output_path = script_dir / '../data/derived-data/cancertype_bar.png'

df = pd.read_csv(data_path)

df["Cancer Type"] = (
    df["Cancer Type"]
        .str.replace("_", " ", regex=False)
        .str.replace("-bronch", "/Bronchus", regex=False)
        .str.replace(" Cancer", "", regex=False)
        .str.title())


base = alt.Chart(df).encode(
    x=alt.X('Cancer Type:N', sort=alt.SortField("Incidence", order='descending'), axis=alt.Axis(labelAngle=-45)),
    y='Incidence:Q'
)

background = base.mark_bar(
    color="#800000",
    opacity=1,
    stroke=None,
    size=40
)

foreground = base.mark_bar(
    color="#800000",
    opacity=1,
    stroke=None,
    size=40
)

chart2 = background + foreground
chart2.properties(width=600, height=400, title='Cancer Incidence in Illinois by Type').save(output_path)