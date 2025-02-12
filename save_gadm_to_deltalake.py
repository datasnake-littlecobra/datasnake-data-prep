import polars as pl
import geopandas as gpd
from deltalake.writer import write_deltalake
from push_to_deltalake_prod import save_to_deltalake_object_storage
from push_to_deltalake_prod import upload_raw_delta_to_s3_prod

# Paths to GADM GeoPackage files
gadm_paths = {
    "ADM0": "geoBoundariesCGAZ_ADM0.gpkg",
    "ADM1": "geoBoundariesCGAZ_ADM1.gpkg",
    "ADM2": "geoBoundariesCGAZ_ADM2.gpkg",
}

# Delta Lake storage paths
deltalake_gadm_paths = {
    "ADM0": "deltalake-gadm/gadm0",
    "ADM1": "deltalake-gadm/gadm1",
    "ADM2": "deltalake-gadm/gadm2",
    "COMBINED": "deltalake-gadm/gadm_combined",
}

deltalake_gadm_s3_uri = {
    "ADM0": f"s3://deltalake-gadm/gadm0",
    "ADM1": f"s3://deltalake-gadm/gadm1",
    "ADM2": f"s3://deltalake-gadm/gadm2",
    "COMBINED": f"s3://deltalake-gadm/gadm_combined",
}

country_code_mapping = {
    "USA": "US",  # Normalize USA to US
    "GBR": "GB",  # United Kingdom
    "DEU": "DE",  # Germany
    "FRA": "FR",  # France
    "ESP": "ES",  # Spain
    "ITA": "IT",  # Italy
    "NLD": "NL",  # Netherlands
    "CHN": "CN",  # China
    "JPN": "JP",  # Japan
    "CAN": "CA",  # Canada
    "AUS": "AU",  # Australia
    "BRA": "BR",  # Brazil
    "IND": "IN",  # India
    "RUS": "RU",  # Russia
    "MEX": "MX",  # Mexico
    "ZAF": "ZA",  # South Africa
}

# ✅ Normalize GADM country codes using the mapping
# gadm_countries_normalized = {country_code_mapping.get(code, code) for code in gadm_countries}


def load_gadm_data(file_path):
    """Load GADM data from a GeoPackage file into a GeoDataFrame."""
    try:
        return gpd.read_file(file_path)
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")
        return None


def convert_gdf_to_polars(gdf, level):
    """Convert a GeoDataFrame to a Polars DataFrame with geometry as WKT."""
    if gdf is None or gdf.empty:
        return None

    if level == "ADM0":
        gdf.rename(columns={"shapeGroup": "country"}, inplace=True)
        # ✅ Normalize country codes using mapping
        gdf["country"] = gdf["country"].apply(lambda x: country_code_mapping.get(x, x))
        gdf.rename(columns={"shapeName": "country_full_name"}, inplace=True)
        gdf["country_full_name"] = gdf["country_full_name"].str.replace(" ", "_")
        gdf["wkt_geometry_country"] = gdf["geometry"].apply(
            lambda geom: geom.wkt if geom else None
        )

    elif level == "ADM1":
        gdf.rename(columns={"shapeName": "state"}, inplace=True)
        gdf["state"] = gdf["state"].str.replace(" ", "_")
        gdf["wkt_geometry_state"] = gdf["geometry"].apply(
            lambda geom: geom.wkt if geom else None
        )
    elif level == "ADM2":
        gdf.rename(columns={"shapeName": "city"}, inplace=True)
        gdf["city"] = gdf["city"].str.replace(" ", "_")
        gdf["wkt_geometry_city"] = gdf["geometry"].apply(
            lambda geom: geom.wkt if geom else None
        )

    df = pl.DataFrame(gdf.drop(columns=["geometry"]))
    df = df.with_columns(pl.lit(level).alias("gadm_level"))
    # print(df.head())
    return df


# def save_to_deltalake(df, delta_path):
#     """Save a Polars DataFrame to Delta Lake."""
#     if df is None or df.is_empty():
#         print(f"⚠ Skipping empty dataframe for {delta_path}")
#         return

#     write_deltalake(delta_path, df.to_arrow(), mode="overwrite")
#     print(f"✅ Saved to {delta_path}")


def process_and_store_gadm():
    """Load, process, and store GADM data into Delta Lake."""
    gadm_dfs = {}

    for level, file_path in gadm_paths.items():
        print(f"📌 Processing {level}...")
        gdf = load_gadm_data(file_path)
        # ✅ Print Unique Country Codes (shapeName contains country names)
        # unique_countries = gdf["shapeGroup"].unique()
        # print("Distinct Country Codes in GADM0:", unique_countries)
        # print(gdf['shapeGroup'])
        # return
        if gdf is not None:
            df = convert_gdf_to_polars(gdf, level)
            # print(df.head())
            save_to_deltalake_object_storage(
                df, deltalake_gadm_s3_uri[level], "overwrite"
            )
            upload_raw_delta_to_s3_prod(df, deltalake_gadm_s3_uri[level])
            gadm_dfs[level] = df
        else:
            print(f"⚠ Skipping {level} due to missing data.")

    if all(level in gadm_dfs for level in ["ADM0", "ADM1", "ADM2"]):
        gadm_combined_df = pl.concat(
            [gadm_dfs["ADM2"], gadm_dfs["ADM1"], gadm_dfs["ADM0"]], how="diagonal"
        )

        gadm_combined_df = gadm_combined_df.sort("gadm_level", descending=True).unique(
            subset=["shapeGroup", "country", "state", "city"], keep="first"
        )

        # save_to_deltalake(gadm_combined_df, deltalake_paths["COMBINED"])


if __name__ == "__main__":
    # distinct_countries()
    # distinct_states()
    # distinct_cities()
    process_and_store_gadm()
