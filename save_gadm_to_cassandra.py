import logging
import polars as pl
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.concurrent import execute_concurrent_with_args

# ✅ Connect to Cassandra
def connect_cassandra(cluster_ips, keyspace):
    logging.info(f"Connecting to Cassandra cluster: {cluster_ips}")
    try:
        USERNAME = "cassandra"
        PASSWORD = "cassandra"
        auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
        cluster = Cluster(cluster_ips, auth_provider=auth_provider)
        session = cluster.connect()
        session.set_keyspace(keyspace)
        logging.info("Connected to Cassandra...")
        return session
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        raise

# ✅ Insert GADM Data
def insert_gadm_data(session, table_name, dataframe, concurrency=20):
    """Batch insert GADM data into Cassandra."""
    query = f"""
    INSERT INTO {table_name} (country, state, city, gadm_level, wkt_geometry) 
    VALUES (?, ?, ?, ?, ?)
    """
    prepared = session.prepare(query)
    
    args = [
        (
            row["country"],
            row["state"] if "state" in row else None,
            row["city"] if "city" in row else None,
            row["gadm_level"],
            row["wkt_geometry_country"] if "wkt_geometry_country" in row else None
        )
        for row in dataframe.iter_rows(named=True)
    ]
    
    results = execute_concurrent_with_args(session, prepared, args, concurrency=concurrency)
    
    for success, result in results:
        if not success:
            logging.error(f"GADM Write failed: {result}")
    logging.info(f"GADM Data Inserted Successfully.")

# ✅ Load Processed GADM Data (Polars)
def load_gadm_polars(filepath):
    return pl.read_parquet(filepath)

# ✅ Main Execution
if __name__ == "__main__":
    cluster_ips = ["127.0.0.1"]  # Update this for your setup
    keyspace = "geospatial_data"
    gadm_table = "gadm_data"
    
    session = connect_cassandra(cluster_ips, keyspace)
    
    # Load GADM processed data (modify path accordingly)
    gadm_df = load_gadm_polars("deltalake-gadm/gadm_combined.parquet")
    
    # Insert into Cassandra
    insert_gadm_data(session, gadm_table, gadm_df)
