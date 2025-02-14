from uuid import uuid4
import datetime
import polars as pl
from cassandra.query import BatchStatement
from concurrent.futures import ThreadPoolExecutor, as_completed
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.concurrent import execute_concurrent_with_args
import logging


def save_to_cassandra_main(df, gadm_level: str):
    # session = None
    try:
        logging.info("Inside Cassandra Connect call:")
        print("Inside Cassandra Connect call with dataframe:")
        print(df.head())
        # logging.info(cluster_ips.split(","))
        # logging.info(keyspace)
        # print(cluster_ips.split(","))
        # print(keyspace)
        # session = connect_cassandra(keyspace)
        USERNAME = "cassandra"
        PASSWORD = "cassandra"
        CASSANDRA_HOSTS = ["127.0.0.1"]
        auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
        cluster = Cluster(
            contact_points=CASSANDRA_HOSTS,
            auth_provider=auth_provider,
            protocol_version=4,  # Stay on version 4
            load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        )
        session = cluster.connect()
        session.set_keyspace("test_keyspace")  # datasnake_data_prep_keyspace
        print("cassandra connection established!")
        # dataframe = pl.DataFrame(
        #     {"stock_id": [uuid4() for _ in range(3)]},
        #     {"symbol": ["AAPL", "MSFT", "GOOG"]},
        #     {"price": [140, 134, 142]},
        #     {"timestamp": [datetime.datetime.now() for _ in range(3)]},
        # )
        print("dataframe is ready!")
        print(df.head())
        print("inserting into stocks table")
        insertquery = "INSERT INTO stocks (stock_id, symbol, price, timestamp) VALUES (uuid(),'AAPL',140,toTimestamp(now())) IF NOT EXISTS"
        # data = [
        #     (row["stock_id"], row["symbol"], row["price"], row["timestamp"])
        #     for row in dataframe.to_dicts()
        # ]
        # for row in data:
        # print(insertquery)
        session.execute(insertquery)
        print("data inserted successfully")
        # insert_sample_data(session)
        # optimized_batch_insert_cassandra(
        #     session, keyspace, gadm_level, df, batch_size=50, sleep_time=0.1
        # )
        # batch_insert_cassandra(session, table_name, dataframe, batch_size, timeout)
        keyspace="datasnake_data_prep_keyspace"
        batch_insert_cassandra_async(session, keyspace, gadm_level, df, concurrency=10)
        # optimized_insert_cassandra(
        #     session, keyspace, gadm_level, df, concurrency=5, batch_size=100
        # )
    except Exception as e:
        logging.error(f"Error writing to Cassandra: {e}")


def insert_sample_data(session):
    try:
        dataframe = pl.DataFrame(
            {"stock_id": [uuid4() for _ in range(3)]},
            {"symbol": ["AAPL", "NSFT", "GOOG"]},
            {"price": [140, 134, 142]},
            {"timestamp": [datetime.datetime.now() for _ in range(3)]},
        )
        print(dataframe.head())
        print("inserting into stocks table")
        insertquery = "INSERT INTO stocks (stock_id, symbol, price, timestamp) VALUES (%s,%s,%s,%s) IF NOT EXISTS"
        data = [
            (row["stock_id"], row["symbol"], row["price"], row["timestamp"])
            for row in dataframe.to_dicts()
        ]
        for row in data:
            session.execute(insertquery, row)
            print("data inserted successfully")
    except Exception as e:
        raise e


def connect_cassandra(keyspace):
    logging.info(f"Connecting to Cassandra cluster: ")
    try:
        """Connect to Cassandra."""
        USERNAME = "cassandra"
        PASSWORD = "cassandra"
        CASSANDRA_HOSTS = ["127.0.0.1"]
        auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
        cluster = Cluster(
            CASSANDRA_HOSTS,
            auth_provider=auth_provider,
            load_balancing_policy=DCAwareRoundRobinPolicy(),
            protocol_version=5,
        )
        session = cluster.connect()
        session.set_keyspace(keyspace)
        return session
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        raise


from cassandra.query import BatchStatement
import logging
import time


def optimized_batch_insert_cassandra(
    session, keyspace, gadm_level, dataframe, batch_size=50, sleep_time=0.1
):
    try:
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
                    "city",
                    "shapeID",
                    "gadm_level",
                    "wkt_geometry_city",
                ],
            },
        }

        table_info = table_mapping[gadm_level]
        table_name = table_info["table_name"]
        columns = table_info["columns"]

        column_names = ", ".join(columns)
        placeholders = ", ".join(["?"] * len(columns))
        insert_query = f"INSERT INTO {keyspace}.{table_name} ({column_names}) VALUES ({placeholders})"

        prepared = session.prepare(insert_query)

        rows = [tuple(row) for row in dataframe.iter_rows()]

        for i in range(0, len(rows), batch_size):
            batch = BatchStatement()
            for row in rows[i : i + batch_size]:
                batch.add(prepared, row)

            session.execute(batch)

            time.sleep(sleep_time)  # Throttle inserts slightly to prevent overloading

        logging.info(f"Successfully inserted {len(rows)} records into {table_name}")

    except Exception as e:
        logging.error(f"Errored out writing to Cassandra: {e}")
        raise


def optimized_insert_cassandra(
    session, keyspace, gadm_level, dataframe, concurrency=5, batch_size=100
):
    try:
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
                    "city",
                    "shapeID",
                    "gadm_level",
                    "wkt_geometry_city",
                ],
            },
        }

        table_info = table_mapping[gadm_level]
        table_name = table_info["table_name"]
        columns = table_info["columns"]

        # Step 4: Generate the INSERT CQL statement dynamically
        column_names = ", ".join(columns)
        placeholders = ", ".join(
            ["?"] * len(columns)
        )  # Cassandra uses %s as placeholders

        insert_query = f"INSERT INTO {keyspace}.{table_name} ({column_names}) VALUES ({placeholders})"
        # print("printing cassandra variables:")
        # print(column_names)
        # print(placeholders)
        # print(insert_query)

        prepared = session.prepare(insert_query)

        args = [
            tuple(row[col] if col in row else None for col in columns)
            for row in dataframe.iter_rows(named=True)
        ]

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = []

            batch_size = 1000
            rows = list(dataframe.iter_rows())

            for i in range(0, len(rows), batch_size):
                batch = BatchStatement()
                for row in rows[i : i + batch_size]:
                    batch.add(prepared, tuple(row))  # Use tuple(row) for batch insert
                    # session.execute(batch)

                futures.append(executor.submit(session.execute, batch))

            for future in as_completed(futures):
                try:
                    future.result()  # Raise any exception
                except Exception as e:
                    logging.error(f"Insert failed: {e}")
    except Exception as e:
        logging.error(f"Errored out writing to cassandra: {e}")
        raise


def row_generator(dataframe, columns):
    for row in dataframe.iter_rows(named=True):
        yield tuple(row[col] if col in row else None for col in columns)


def batch_insert_cassandra_async(
    session, keyspace, gadm_level, dataframe, concurrency=20
):
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

        insert_query = f"INSERT INTO {keyspace}.{table_name} ({column_names}) VALUES ({placeholders})"
        # print("printing cassandra variables:")
        # print(column_names)
        # print(placeholders)
        # print(insert_query)

        prepared = session.prepare(insert_query)

        args = [
            tuple(row[col] if col in row else None for col in columns)
            for row in dataframe.iter_rows(named=True)
        ]

        # print("printing args:")
        # for arg in args:
        #     print(arg)

        # results = execute_concurrent(session, [(prepared, row) for row in args], concurrency=concurrency)
        results = execute_concurrent_with_args(
            session,
            prepared,
            row_generator(dataframe, columns),
            concurrency=concurrency,
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
