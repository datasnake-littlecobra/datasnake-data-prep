from uuid import uuid4
import datetime
import polars as pl
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import logging


def save_to_cassandra_main(df, cluster_ips, keyspace="test_keyspace"):
    session = None
    try:
        logging.info("Inside Cassandra Connect call:")
        print("Inside Cassandra Connect call:")
        logging.info(cluster_ips.split(","))
        logging.info(keyspace)
        print(cluster_ips.split(","))
        print(keyspace)
        # session = connect_cassandra(keyspace)
        USERNAME = "cassandra"
        PASSWORD = "cassandra"
        CASSANDRA_HOSTS = ["127.0.0.1"]
        auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
        cluster = Cluster(CASSANDRA_HOSTS, auth_provider=auth_provider)
        session = cluster.connect()
        session.set_keyspace(keyspace)
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
        # insert_sample_data(session)
        # optimized_batch_insert_cassandra(
        #     session, keyspace, gadm_level, df, batch_size=50, sleep_time=0.1
        # )
        # batch_insert_cassandra(session, table_name, dataframe, batch_size, timeout)
        # batch_insert_cassandra_async(session, keyspace, gadm_level, df, concurrency=10)
        # optimized_insert_cassandra(
        #     session, keyspace, gadm_level, df, concurrency=5, batch_size=100
        # )
    except Exception as e:
        logging.error(f"Error writing to Cassandra: {e}")
    finally:
        if session is not None:  # Check if session was successfully created
            logging.info("Closing Cassandra session...")
            # session.shutdown()
            raise
