from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
import logging


def save_to_cassandra_main(df, cluster_ips, keyspace, gadm_level):
    logging.info("Inside Cassandra Connect call:")
    logging.info(cluster_ips.split(","))
    logging.info(keyspace)
    # session = connect_cassandra(cluster_ips.split(","), keyspace)
    # batch_insert_cassandra(session, table_name, dataframe, batch_size, timeout)
    batch_insert_cassandra_async(gadm_level, df, concurrency=10)


def connect_cassandra(cluster_ips, keyspace):
    logging.info(f"Connecting to Cassandra cluster: {cluster_ips}")
    try:
        """Connect to Cassandra."""
        USERNAME = "cassandra"
        PASSWORD = "cassandra"
        auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
        cluster = Cluster(
            cluster_ips, auth_provider=auth_provider
        )  # Replace with container's IP if needed
        session = cluster.connect()
        session.set_keyspace(keyspace)
        logging.info("Connected to cassandra...")
        return session
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        raise


def batch_insert_cassandra_async(gadm_level, dataframe, concurrency=20):
    try:
        """Insert data into Cassandra asynchronously."""
        # Step 2: Define table structures dynamically
        table_mapping = {
            "ADM0": {
                "table_name": "gadm0_data",
                "columns": [
                    "country_code",
                    "country_full_name",
                    "gadm_level",
                    "wkt_geometry_country",
                ],
            },
            "ADM1": {
                "table_name": "gadm1_data",
                "columns": [
                    "country_code",
                    "state",
                    "shapeID",
                    "gadm_level",
                    "wkt_geometry_state",
                ],
            },
            "ADM2": {
                "table_name": "gadm2_data",
                "columns": [
                    "country_code",
                    "state",
                    "city",
                    "shapeID",
                    "gadm_level",
                    "wkt_geometry_city",
                ],
            },
        }
        # Process in chunks of 10,000 records
        # chunk_size = 10000
        # for i in range(0, len(dataframe), chunk_size):
        #     chunk = dataframe[i:i + chunk_size]
        #     async_insert_cassandra(session, table_name, chunk, concurrency=20)
        # logging.info(f"Starting to write into cassandra: {table_name}")
        table_info = table_mapping[gadm_level]
        table_name = table_info["table_name"]
        columns = table_info["columns"]

        # Step 4: Generate the INSERT CQL statement dynamically
        column_names = ", ".join(columns)
        placeholders = ", ".join(
            ["?"] * len(columns)
        )  # Cassandra uses %s as placeholders

        insert_query = (
            f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
        )
        print("printing cassandra variables:")
        print(column_names)
        print(placeholders)
        print(insert_query)
        return
        # prepared = session.prepare(insert_query)
        # args = [
        #     (
        #         row["country"],
        #         row["state"] if "state" in row else None,
        #         row["city"] if "city" in row else None,
        #         row["gadm_level"],
        #         row["wkt_geometry_country"] if "wkt_geometry_country" in row else None,
        #     )
        #     for row in dataframe.iter_rows(named=True)
        # ]

        args = [
            tuple(row[col] if col in row else None for col in columns)
            for _, row in dataframe.iterrows()
        ]

        # results = execute_concurrent(session, [(prepared, row) for row in args], concurrency=concurrency)
        results = execute_concurrent_with_args(
            session, prepared, args, concurrency=concurrency
        )

        # Log any errors
        for success, result in results:
            if success:
                logging.info("Cassandra: GADM data - Inserted all rows asynchronously.")
            if not success:
                logging.error(f"GADM Write failed: {result}")

    except Exception as e:
        logging.error(f"Errored out writing to cassandra: {e}")
        raise
