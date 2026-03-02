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

ghgp = pd.read_excel(raw_ghgp)

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

ghgp = ghgp.groupby("County", as_index=False).agg({
    "Total Direct Emissions": "sum",
    "Latitude": "mean",
    "Longitude": "mean"
})

ghgp = ghgp[ghgp["County"] != "Lasalle"]

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

# Loading Stroke file

raw_stroke = script_dir / '../data/raw-data/heart_stroke_mortality.csv'
output_stroke = script_dir / '../data/derived-data/stroke.csv'

# Cleaning Stroke file

stroke = pd.read_csv(raw_stroke)

stroke = stroke[stroke["Year"] == "2019"]
stroke = stroke[stroke["Topic"] == "All stroke"]

stroke = stroke[["Year", "LocationAbbr", "LocationDesc", "Topic", "Data_Value"]]
stroke.rename(columns={"LocationAbbr": "State", "LocationDesc": "County"}, inplace=True)

stroke = stroke.dropna()
stroke = stroke.groupby("County", as_index=False).agg({"Data_Value": "sum"})

stroke.to_csv(output_stroke, index=False)

# Loading Heart file

raw_heart = script_dir / '../data/raw-data/heart_stroke_mortality.csv'
output_heart = script_dir / '../data/derived-data/heart.csv'

# Cleaning Heart file

heart = pd.read_csv(raw_heart)

heart = heart[heart["Year"] == "2019"]
heart = heart[heart["Topic"] == "All heart disease"]

heart = heart[["Year", "LocationAbbr", "LocationDesc", "Topic", "Data_Value"]]
heart.rename(columns={"LocationAbbr": "State", "LocationDesc": "County"}, inplace=True)

heart = heart.dropna()
heart = heart.groupby("County", as_index=False).agg({"Data_Value": "sum"})

heart.to_csv(output_heart, index=False)

# Loading COVID file

raw_covid = script_dir / '../data/raw-data/us-counties.csv'
output_covid = script_dir / '../data/derived-data/covid.csv'

# Cleaning COVID file

cov = pd.read_csv(raw_covid)

cov["Date"] = 2020

cov = cov[["Date", "County", "State", "Deaths"]]

cov = cov.groupby("County", as_index=False).agg({"Deaths": "sum"})

covid.to_csv(output_covid, index=False)

# Loading Population file

raw_pop = script_dir / '../data/raw-data/population_illinois.xlsx'
output_pop = script_dir / '../data/derived-data/population.csv'

#Illinois Population by County

pop = pd.read_excel(raw_pop)

pop["County"] = (pop["County"]
                  .astype(str)
                  .str.strip()
                  .str.lower()
                  .str.replace(r"county.*$", "", regex=True)
                  .str.replace(r"\s+", " ", regex=True)
                  .str.replace(".", "")
                  .str.strip()
                  .str.title()
)

pop.to_csv(output_pop, index=False)

#Merged Health Outcomes

output_outcomes = script_dir / '../data/derived-data/outcomes.csv'

ghgp_20 = ghgp.sort_values(by="Total Direct Emissions", ascending=False).head(20).reset_index().copy()
top_20 = ghgp_20["County"].unique()
asthma_20 = asthma_filtered[asthma_filtered["County"].isin(top_20)].copy()
copd_20 = copd_filtered[copd_filtered["County"].isin(top_20)].copy()
covid_20 = cov[cov["County"].isin(top_20)].copy()
heart_20 = heart[heart["County"].isin(top_20)].copy()
stroke_20 = stroke[stroke["County"].isin(top_20)].copy()

asthma_20 = asthma_20.rename(columns={"Value": "Asthma Incidence"})
copd_20 = copd_20.rename(columns={"Value": "COPD Deaths"})
covid_20 = cov_20.rename(columns={"Deaths": "COVID Deaths"})
heart_20 = heart_20.rename(columns={"Data_Value": "Heart Failures"})
stroke_20 = stroke_20.rename(columns={"Data_Value": "Stroke Deaths"})
pop_20 = pop[pop["County"].isin(top_20)].copy()

ghgp_20 = ghgp_20[["County", "Total Direct Emissions"]]
asthma_20 = asthma_20[["County", "Asthma Incidence"]]
copd_20 = copd_20[["County", "COPD Deaths"]]

outcomes = ghgp_20.copy()

outcomes = outcomes.merge(asthma_20, on="County", how="left")
outcomes = outcomes.merge(copd_20, on="County", how="left")
outcomes = outcomes.merge(covid_20, on="County", how="left")
outcomes = outcomes.merge(heart_20, on="County", how="left")
outcomes = outcomes.merge(stroke_20, on="County", how="left")
outcomes = outcomes.merge(pop_20, on="County", how="left")
                          
outcomes.to_csv(output_outcomes, index=False)


