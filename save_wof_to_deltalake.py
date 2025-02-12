import duckdb
import geopandas as gpd
import polars as pl
import json
from shapely.geometry import shape
from datetime import datetime
from deltalake.writer import write_deltalake
import sys

sys.stdout.reconfigure(encoding="utf-8")

# ✅ Load GADM1 State Boundaries
gadm1_path = "geoBoundariesCGAZ_ADM1.gpkg"
gadm1_gdf = gpd.read_file(gadm1_path)[["shapeName", "geometry"]]
gadm1_gdf.rename(columns={"shapeName": "state"}, inplace=True)
print("GADM1 State Polygons Loaded.")

# ✅ Load GADM1 State Boundaries
gadm2_path = "geoBoundariesCGAZ_ADM2.gpkg"
gadm2_gdf = gpd.read_file(gadm2_path)[["shapeName", "geometry"]]
gadm2_gdf.rename(columns={"shapeName": "city"}, inplace=True)
print("GADM2 State Polygons Loaded.")

# ✅ Configurable Parameters
wof_db_path = "C://Users//sterr//Downloads/whosonfirst-data-postalcode-latest.db"
chunk_size = 100000  # Adjust this to control chunk size
test_mode = True  # Process only the first chunk if True

# ✅ Open DuckDB Connection
con = duckdb.connect(wof_db_path, read_only=True)
con.execute("INSTALL json; LOAD json;")
con.execute("INSTALL spatial; LOAD spatial;")
con.execute("INSTALL delta;")
con.execute("LOAD delta;")

con.execute(f"ATTACH DATABASE '{wof_db_path}' AS wofdb (TYPE sqlite);")

# ✅ Get Total Record Count (for Chunking)
total_records = con.execute("SELECT COUNT(*) FROM wofdb.spr").fetchone()[0]
num_chunks = (total_records // chunk_size) + 1
print(
    f"Total Records: {total_records} | Processing in {num_chunks} Chunks of {chunk_size} each."
)

distinct_countries = con.execute("SELECT DISTINCT(country) FROM wofdb.spr").fetchall()
for record in distinct_countries:
    print(f"Total DISTINCT COUNTRIES: {distinct_countries} ")


# distinct countries
# distinct

print(hello)


# ✅ Function to Extract Geometry & City Name from JSON
def parse_geojson(json_str):
    """Parses GeoJSON string and extracts geometry & city name."""
    try:
        geojson_obj = json.loads(json_str)  # Convert from string to dict
        geometry = shape(geojson_obj["geometry"])  # Extract geometry as Shapely object
        city = geojson_obj["properties"].get("mz:postal_locality", None)  # Extract city
        # print("printing geojson_obj from geojson body to see city", geojson_obj)
        # Extract lat/lon from properties
        lat = geojson_obj["properties"].get("geom:latitude", None)
        lon = geojson_obj["properties"].get("geom:longitude", None)

        return geometry, city, lat, lon
    except (json.JSONDecodeError, KeyError, TypeError):
        return None, None  # Return None if parsing fails


# ✅ Processing Function
def processAndStoreWofToDeltaLakeInChunks(num_chunks):
    """Processes WOF data in chunks, extracts geometry & state, and saves to DeltaLake."""
    for chunk_index in range(num_chunks):
        t1 = datetime.now()
        print(f"\nProcessing Chunk {chunk_index + 1}/{num_chunks}...")

        # ✅ Read chunk from WOF `spr` & `geojson`
        wof_query = f"""
            SELECT 
            spr.id,
            spr.country,
            spr.name AS postal_code,
            geo.body AS geojson_data
            FROM spr AS spr
            JOIN geojson AS geo ON spr.id = geo.id
            WHERE spr.country IN ('USA', 'US')
            LIMIT {chunk_size} OFFSET {chunk_index * chunk_size};
        """

        # for testing in Illinois, Sangaman
        # WHERE spr.country = 'USA' AND spr.name IN ('62701', '62702', '62703', '62704', '62705', '62706', '62707', '62708', '62711', '62712', '62715', '62716', '62719', '62721', '62722', '62723', '62726', '62736', '62739', '62746', '62756', '62757', '62761', '62762', '62763', '62764', '62765', '62766', '62767', '62769', '62776', '62777', '62781', '62786', '62791', '62794', '62796')
        # ✅ Execute DuckDB Query & Convert to Polars
        wof_df = con.execute(wof_query).pl()
        wof_df.write_csv("test_wof.csv", separator=",")

        if wof_df.is_empty():
            print(f"No more records. Stopping at chunk {chunk_index}.")
            break

        # ✅ Apply Parsing Functions (Extract Geometry & City)
        geometry_list = []
        city_list = []
        lat_list = []
        lon_list = []

        print("looks find until here 1")

        for geojson_str in wof_df["geojson_data"]:
            geometry, city, lat, lon = parse_geojson(geojson_str)
            geometry_list.append(geometry)
            city_list.append(city)
            lat_list.append(lat)
            lon_list.append(lon)

        print("looks find until here 2")
        # Replace None with np.nan before creating the Polars Series
        lat_list = [float(x) if x is not None else float("nan") for x in lat_list]
        lon_list = [float(x) if x is not None else float("nan") for x in lon_list]

        print(lat_list)

        # ✅ Add New Columns to DataFrame
        wof_df = wof_df.with_columns(
            [
                pl.Series("geometry", geometry_list),
                pl.Series("city", city_list),
                pl.Series("lat", lat_list).cast(pl.Float64),
                pl.Series("lon", lon_list).cast(pl.Float64),
            ]
        )

        print("wof_df extract geometry df:")
        print(wof_df.head())

        # ✅ Convert to GeoPandas for Spatial Join
        # wof_gdf = gpd.GeoDataFrame(
        #     wof_df.to_pandas(),
        #     geometry=wof_df["geometry"],
        #     crs="EPSG:4326",
        # )
        # wof_gdf = gpd.GeoDataFrame(
        #     wof_df.to_pandas(),
        #     geometry=gpd.GeoSeries(wof_df["geometry"], crs="EPSG:4326"),
        # )
        # ✅ Convert Polars to GeoPandas with Correct CRS
        wof_gdf = gpd.GeoDataFrame(
            wof_df.to_pandas(),
            geometry=gpd.GeoSeries(wof_df["geometry"].to_list(), crs="EPSG:4326"),
        )

        # ✅ Filter Out Invalid Points (0,0)
        # wof_gdf = wof_gdf[~((wof_gdf.geometry.x == 0) & (wof_gdf.geometry.y == 0))]

        # ✅ If wof_gdf is empty after filtering, SKIP processing this chunk
        if wof_gdf.empty:
            print("Skipping this chunk because wof_gdf is empty after filtering.")
            continue  # Move to next chunk

        # ✅ Convert GeoPandas → Pandas → Polars Safely
        # wof_gdf = wof_gdf.astype(str)  # Ensure all columns are strings to avoid dtype issues
        # final_df = pl.DataFrame(wof_gdf.to_dict(orient="list"))

        print(wof_df.head())

        # ✅ Ensure Both DataFrames Have the Same CRS Before Intersection
        if wof_gdf.crs != gadm1_gdf.crs:
            wof_gdf = wof_gdf.to_crs(gadm1_gdf.crs)

        if wof_gdf.crs != gadm2_gdf.crs:
            wof_gdf = wof_gdf.to_crs(gadm2_gdf.crs)

        # ✅ Spatial Join to Find State (GADM1)
        wof_gdf = gpd.sjoin(wof_gdf, gadm1_gdf, how="left", predicate="intersects")

        print("wof_gdf after sjoin with gadm1")
        print(wof_gdf.head())

        # ✅ Keep Only Geometry for Spatial Join (Find City from GADM2)
        wof_gdf_city = gpd.sjoin(
            wof_gdf[["geometry"]], gadm2_gdf, how="left", predicate="intersects"
        )

        print("wof_gdf_city after sjoin with gadm2")
        print(wof_gdf_city.head())

        # ✅ Debug Check (Ensure `city` Exists in `wof_gdf_city`)
        print("wof_gdf_city.head() before merge:")
        print(wof_gdf_city.head())

        # ✅ Remove Duplicates from City Join
        wof_gdf_city = wof_gdf_city.drop_duplicates(subset=["geometry"])

        print("wof_gdf_city.head() after dropping geometry dups:")
        print(wof_gdf_city.head())

        # ✅ Merge City Back into Main DataFrame
        wof_gdf = wof_gdf.merge(
            wof_gdf_city[["geometry", "city"]], on="geometry", how="left"
        )
        print("wof_gdf.head() after merging geometry and city:")
        print(wof_gdf.head())

        # ✅ Fill Missing City Values from GADM2
        wof_gdf["city"] = wof_gdf["city_x"].fillna(wof_gdf["city_y"])
        wof_gdf = wof_gdf.drop(columns=["city_x", "city_y"])  # Clean Up Extra Columns

        # ✅ Log & Debug: Find Mismatched Country/State/City
        # mismatched = wof_gdf[
        #     (wof_gdf["state"].isna()) | (wof_gdf["city"].isna())  # Find Missing States or Cities
        # ]
        # print("Sample GADM vs WOF Mismatches:")
        # print(mismatched[["country", "state", "city", "postal_code"]].head(5))

        # ✅ Extract lat, lon from Point() Geometry
        # wof_gdf["lat"] = wof_gdf.geometry.y  # Extract latitude
        # wof_gdf["lon"] = wof_gdf.geometry.x  # Extract longitude

        # ✅ Convert Geometry to WKT (Well-Known Text) Format
        wof_gdf["wkt_geometry"] = wof_gdf["geometry"].apply(
            lambda geom: geom.wkt if geom else None
        )
        # ✅ Drop Original Geometry Column
        wof_gdf = wof_gdf.drop(columns=["geometry"])

        wof_gdf.drop(columns=["geojson_data"], inplace=True)

        print("wof_gdf.head() after dropping geojson_data:")
        print(wof_gdf.head())

        # ✅ Ensure All Required Columns Exist
        required_columns = [
            "country",
            "state",
            "city",
            "postal_code",
            "lat",
            "lon",
            "wkt_geometry",
        ]
        wof_gdf = wof_gdf[required_columns]  # Keep only relevant columns
        print("before polar conversion: ", wof_gdf[required_columns].head(5))

        # ✅ Convert Back to Polars for Efficient Storage
        final_df = pl.DataFrame(wof_gdf)

        # ✅ Replace spaces with underscores in partitioning columns
        final_df = final_df.with_columns(
            [
                pl.col("country").str.replace_all(" ", "_"),
                pl.col("state").str.replace_all(" ", "_"),
                pl.col("city").str.replace_all(" ", "_"),
            ]
        )

        # ✅ Print Sample Output
        print("Final dataframe:", len(final_df))
        print(final_df[required_columns].head(10))
        
        # ✅ Save to Delta Lake with Partitioning
        write_deltalake(
            "deltalake-wof",
            final_df.to_arrow(),
            mode="append",
            partition_by=["country", "state", "city"],
        )

        print(f"Chunk {chunk_index + 1} saved to Delta Lake.")

        t2 = datetime.now()
        total = t2 - t1
        print(f"it took {total} to run this chunk. {chunk_index + 1}")

        # ✅ If testing, stop after first chunk
        if test_mode:
            print("Test Mode Enabled: Only 1st chunk processed.")
            break


# ✅ Run Processing
processAndStoreWofToDeltaLakeInChunks(num_chunks)
print("Data Processing Complete!")
