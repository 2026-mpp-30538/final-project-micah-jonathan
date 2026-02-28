import geopandas as gpd
import pandas as pd
from pathlib import Path
from shapely import wkt

script_dir = Path(__file__).parent

# Creating single file of emissions related cancers by county for 2016-2020  
files = [
    "lung-bronch_cancer",
    "breast_cancer",
    "colorectal_cancer",
    "kidney_cancer",
    "bladder_cancer"]

combined_df = pd.DataFrame()

for file in files:
    raw_cancer = script_dir / f"../data/raw-data/{file}.csv"
    output_cancer = script_dir / f"../data/derived-data/{file}_filtered.csv"
    
    df = pd.read_csv(raw_cancer)

    cancer_col = df.columns[6]

    df = df[["CountyFIPS", "County", cancer_col]]
    
    df = df.rename(columns={cancer_col: file})
    
    if combined_df.empty:
        combined_df = df
    else:
        combined_df = pd.merge(combined_df, df, on=["CountyFIPS", "County"], how="outer")

final_output = script_dir / "../data/derived-data/combined_cancer_by_county.csv"

combined_df.to_csv(final_output, index=False)

# Cleaning crude prevalence of COPD by county for 2023 
raw_copd = script_dir / '../data/raw-data/crude_COPD.csv'
output_copd = script_dir / '../data/derived-data/COPD_filtered.csv'

copd_df = pd.read_csv(raw_copd)

copd_filtered = copd_df[["CountyFIPS", "County", "Year", "Value"]]

copd_filtered.to_csv(output_copd, index=False)

# Cleaning crude prevalance of asthma by county for 2023
raw_asthma = script_dir / '../data/raw-data/crude_asthma.csv'
output_asthma = script_dir / '../data/derived-data/asthma_filtered.csv'

asthma_df = pd.read_csv(raw_asthma)

asthma_filtered = asthma_df[["CountyFIPS", "County", "Year", "Value"]]

asthma_filtered.to_csv(output_asthma, index=False)

# Loading carbon emissions file

raw_ghgp = script_dir / '../data/raw-data/ghgp_data_2021_0.xlsx'
output_ghgp = script_dir / '../data/derived-data/ghgp.csv'

# Cleaning carbon emissions

ghgp = pd.read_csv(raw_ghgp)

ghgp = ghgp.iloc[3:].reset_index(drop=True)

ghgp = ghgp[["Unnamed: 4", "Unnamed: 7", "Unnamed: 8", "Unnamed: 9", "Unnamed: 13"]]
ghgp.rename(columns={"Unnamed: 4": "State", "Unnamed: 7": "County", "Unnamed: 8": "Latitude", "Unnamed: 9": "Longitude", "Unnamed: 13": "Total Direct Emissions"}, inplace=True)

ghgp = ghgp[ghgp["State"] == "IL"]
ghgp = ghgp.dropna()

ghgp["County"] = (ghgp["County"]
                  .astype(str)
                  .str.strip()
                  .str.lower()
                  .str.replace(r"county.*$", "", regex=True)
                  .str.replace(r"\s+", " ", regex=True)
                  .str.strip()
                  .str.title()
)

ghgp = ghgp.groupby("County").agg({
    "Total Direct Emissions": "sum",
    "Latitude": "mean",
    "Longitude": "mean"
})

ghgp.to_csv(output_ghgp, index=False)

# Loading AQI file

raw_aqi = script_dir / '../data/raw-data/daily_aqi_by_county_2025.csv'
output_aqi = script_dir / '../data/derived-data/aqi.csv'

# Cleaning AQI file

aqi = pd.read_csv(raw_aqi)

aqi = aqi[["State Name", "county Name", "AQI", "Defining Parameter"]]
aqi.rename(columns={"State Name": "State", "county Name": "County"}, inplace=True)

aqi = aqi[aqi["State"] == "Illinois"]

aqi = aqi.groupby(["County", "Defining Parameter"], as_index=False).agg({"AQI": "mean"})

aqi.to_csv(output_aqi, index=False)
