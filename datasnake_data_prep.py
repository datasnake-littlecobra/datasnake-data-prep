import os
import logging

# import argparse
import time
from datetime import datetime
from datetime import timedelta
import polars as pl
import geopandas as gpd
from deltalake.writer import write_deltalake
from save_gadm_to_cassandra import save_to_cassandra_main
from DataFrameCache import DataFrameCache

# from push_to_deltalake_prod import save_to_deltalake_local
# from push_to_deltalake_prod import upload_raw_delta_to_s3_prod

# ✅ Set up logging to BOTH Console & File
logger = logging.getLogger("GADM")
logger.setLevel(logging.INFO)  # Log everything including debug

# ✅ Add File Handler
file_handler = logging.FileHandler(
    "pipeline.log", mode="a", encoding="utf-8"
)  # Append mode
file_handler.setLevel(logging.INFO)  # Capture debug and above

# ✅ Add Console Handler (for real-time logs)
console_handler = logging.StreamHandler()
# console_handler.setLevel(logging.INFO)

# ✅ Define Log Format
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
# console_handler.setFormatter(formatter)

# ✅ Attach Handlers to Logger
logger.addHandler(file_handler)
# logger.addHandler(console_handler)

# Paths to GADM GeoPackage files
gadm_paths_datasnake = {
    "ADM0": "/home/resources/geoBoundariesCGAZ_ADM0.gpkg",
    "ADM1": "/home/resources/geoBoundariesCGAZ_ADM1.gpkg",
    "ADM2": "/home/resources/geoBoundariesCGAZ_ADM2.gpkg",
}

# Delta Lake storage paths
deltalake_gadm_paths = {
    "ADM0": "deltalake-gadm/gadm0",
    "ADM1": "deltalake-gadm/gadm1",
    "ADM2": "deltalake-gadm/gadm2",
    "COMBINED": "deltalake-gadm/gadm_combined",
}
deltalake_partitions = {
    "ADM0": [],
    "ADM1": ["country_code"],
    "ADM2": ["country_code"],
}
dataframe_mapping = {
    "ADM0": [
        "country_code",
        "country_full_name",
        "gadm_level",
        "wkt_geometry_country",
    ],
    "ADM1": ["country_code", "state", "shapeID", "gadm_level", "wkt_geometry_state"],
    "ADM2": [
        "country_code",
        "city",
        "shapeID",
        "gadm_level",
        "wkt_geometry_city",
    ],
}
cassandra_table_names = {
    "ADM0": "gadm0_data",
    "ADM1": "gadm1_data",
    "ADM2": "gadm2_data",
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

# Initialize cache
cache = DataFrameCache(expiration_minutes=15)  # In-memory caching system


def load_gadm_data(file_path):
    """Load GADM data from a GeoPackage file into a GeoDataFrame with caching."""

    # Read the file (only if cache is empty)
    cache_key = f"gadm_{file_path}"
    # ✅ Step 1: Check if data is already cached
    cached_data = cache.get(cache_key)
    if cached_data is not None:
        print(f"Using cached data for: {file_path}")
        return cached_data  # Return cached DataFrame

    # Check if file exists
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        logger.info(f"File not found: {file_path}")
        return None

    try:
        print("PREFER reading file instead:")
        df = gpd.read_file(file_path)  # Read file
        print("setting the cache")
        cache.set(cache_key, df)  # Cache the DataFrame
        print("check cache:")
        print(cache.get(cache_key))
        print(f"Data loaded and cached for: {file_path}")
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return None


def convert_gdf_to_polars(gdf, level):
    """Convert a GeoDataFrame to a Polars DataFrame with geometry as WKT."""
    if gdf is None or gdf.empty:
        return None

    if level == "ADM0":
        gdf.rename(columns={"shapeGroup": "country_code"}, inplace=True)
        gdf.rename(columns={"shapeType": "gadm_level"}, inplace=True)
        # ✅ Normalize country codes using mapping
        gdf["country_code"] = gdf["country_code"].apply(
            lambda x: country_code_mapping.get(x, x)
        )
        gdf.rename(columns={"shapeName": "country_full_name"}, inplace=True)
        gdf["country_full_name"] = gdf["country_full_name"].str.replace(" ", "_")
        gdf["wkt_geometry_country"] = gdf["geometry"].apply(
            lambda geom: geom.wkt if geom else None
        )
        # gdf.drop(columns=["geometry"])
        gdf = gdf[dataframe_mapping["ADM0"]]

    elif level == "ADM1":
        gdf.rename(columns={"shapeGroup": "country_code"}, inplace=True)
        gdf.rename(columns={"shapeType": "gadm_level"}, inplace=True)
        gdf.rename(columns={"shapeName": "state"}, inplace=True)
        gdf["state"] = gdf["state"].str.replace(" ", "_")
        gdf["wkt_geometry_state"] = gdf["geometry"].apply(
            lambda geom: geom.wkt if geom else None
        )
        # gdf.drop(columns=["geometry"])
        gdf = gdf[dataframe_mapping["ADM1"]]

    elif level == "ADM2":
        gdf.rename(columns={"shapeGroup": "country_code"}, inplace=True)
        gdf.rename(columns={"shapeType": "gadm_level"}, inplace=True)
        gdf.rename(columns={"shapeName": "city"}, inplace=True)
        gdf["city"] = gdf["city"].str.replace(" ", "_")
        gdf["wkt_geometry_city"] = gdf["geometry"].apply(
            lambda geom: geom.wkt if geom else None
        )
        # gdf.drop(columns=["geometry"])
        gdf = gdf[dataframe_mapping["ADM2"]]

    df = pl.DataFrame(gdf)
    df.filter(df["country_code"].is_not_null())
    # df = df.with_columns(pl.lit(level).alias("gadm_level"))
    # print(df.head())
    return df


def process_gadm_level(level: str):
    """Process a single GADM level and store it, measuring time and size."""
    print(f"Processing {level} with file positioned at {gadm_paths_datasnake[level]}")
    logger.info(
        f"Processing {level} with file positioned at {gadm_paths_datasnake[level]}"
    )

    start_time = time.time()  # Start timing
    gdf = load_gadm_data(gadm_paths_datasnake[level])

    if gdf is not None:
        df = convert_gdf_to_polars(gdf, level)
        import gc

        del gdf  # Remove the GeoDataFrame to free up memory
        gc.collect()  # Force garbage collection

        print(df.head())
        # ✅ Measure Size
        df_size_mb = df.estimated_size() / (1024 * 1024)  # Convert bytes to MB
        row_count = df.shape[0]

        # ✅ Measure Time
        end_time = time.time()
        time_taken = end_time - start_time

        print(
            f"Processed {level}: {row_count} rows, {df_size_mb:.2f} MB in {time_taken:.2f} sec"
        )
        logger.info(
            f"{level}: {row_count} rows, {df_size_mb:.2f} MB, {time_taken:.2f} sec"
        )

        logger.info(
            f"Uploading the raw delta lake to Object Storage...{deltalake_gadm_s3_uri[level]} , with partitions as {deltalake_partitions[level]}"
        )

        # upload_raw_delta_to_s3_prod(
        #     df, deltalake_gadm_s3_uri[level], deltalake_partitions[level]
        # )

        # Cassandra
        start_time = time.time()
        cluster_ips = "127.0.0.1"
        keyspace = "datasnake_data_prep_keyspace"
        print("sending dataframe to cassandra main:")
        print(df.head())
        save_to_cassandra_main(df, level)
        end_time = time.time()
        time_taken = end_time - start_time
        logger.info(f"Total time take to store gadm_{level} : {time_taken}")
    else:
        print(f"Skipping {level} due to missing data.")
        logger.warning(f"Skipping {level} due to missing data.")


def process_all_gadm_levels():
    """Process and store each GADM level one at a time."""
    for level in gadm_paths_datasnake.keys():
        process_gadm_level(level)  # Process and store one level at a time


# ✅ Run the Prefect Flow
if __name__ == "__main__":
    process_all_gadm_levels()
