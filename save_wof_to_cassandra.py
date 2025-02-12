import logging
import polars as pl
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.concurrent import execute_concurrent_with_args


# ✅ Insert WOF Data
def insert_wof_data(session, table_name, dataframe, concurrency=20):
    """Batch insert WOF data into Cassandra."""
    query = f"""
    INSERT INTO {table_name} (postal_code, country, state, city, lat, lon, wkt_geometry) 
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    prepared = session.prepare(query)

    args = [
        (
            row["postal_code"],
            row["country"],
            row["state"],
            row["city"],
            row["lat"],
            row["lon"],
            row["wkt_geometry"],
        )
        for row in dataframe.iter_rows(named=True)
    ]

    results = execute_concurrent_with_args(
        session, prepared, args, concurrency=concurrency
    )

    for success, result in results:
        if not success:
            logging.error(f"WOF Write failed: {result}")
    logging.info(f"WOF Data Inserted Successfully.")


# ✅ Load Processed WOF Data (Polars)
def load_wof_polars(filepath):
    return pl.read_parquet(filepath)


# ✅ Main Execution
def main(cluster_ips, keyspace, table_name, dataframe, batch_size, timeout):
    # cluster_ips = ["127.0.0.1"]
    # keyspace = "geospatial_data"
    # wof_table = "wof_data"

    session = connect_cassandra(cluster_ips, keyspace)

    # Load WOF processed data
    wof_df = load_wof_polars("deltalake-wof.parquet")

    # Insert into Cassandra
    insert_wof_data(session, wof_table, wof_df)
