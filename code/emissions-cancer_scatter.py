import pandas as pd
from pathlib import Path
import numpy as np
import altair as alt

script_dir = Path(__file__).parent
output_path = script_dir / '../data/derived-data/emissions-cancer_scatter.png'

cancer_data_path = script_dir / '../data/derived-data/combined_cancer_by_county.csv'
cancer_df = pd.read_csv(cancer_data_path)

pop_data_path=script_dir / '../data/derived-data/population.csv'
pop_df = pd.read_csv(pop_data_path)

cancer_df["Cancer Type"] = (
    cancer_df["Cancer Type"]
        .str.replace("_", " ", regex=False)
        .str.replace("-bronch", "/Bronchus", regex=False)
        .str.replace(" Cancer", "", regex=False)
        .str.title())

cancer_rates_df = cancer_df.merge(pop_df, on='County', how="outer")
cancer_rates_df = cancer_rates_df.dropna()
cancer_rates_df["Incidence Rate"] = (cancer_rates_df["Incidence"] / cancer_rates_df["Population"]) * 100

ghgp_data_path = script_dir / '../data/derived-data/ghgp.csv'
emissions_df = pd.read_csv(ghgp_data_path)

ghgp_20 = emissions_df.sort_values(by="Total Direct Emissions", ascending=False).head(20).reset_index().copy()
top_20 = ghgp_20["County"].unique()

cancer_20 = cancer_rates_df[cancer_rates_df["County"].isin(top_20)].copy()

cancer_emissions_df = cancer_20.merge(ghgp_20, on='County', how="outer")
cancer_emissions_df["County"].nunique()

cancer_emissions_df.sort_values(by=["Incidence", "Total Direct Emissions"], ascending=False)

scatter = alt.Chart(cancer_emissions_df).mark_circle(filled=True, size=50).encode(
    y=alt.Y("Incidence Rate:Q", title="Cancer Incidence"),
    x=alt.X("Total Direct Emissions:Q", title="Total Direct Emissions"),
    color=alt.Color(
        "Cancer Type:N",
        title="Cancer Type",
        scale=alt.Scale(range=[
    "#EAAA00", "#275D38", "#007396",
    "#59315F", "#A4343A"]))
).properties(
    width=500,
    height=600,
    title="Cancer Incidence and Total Direct Emissions")
scatter.save(output_path)
