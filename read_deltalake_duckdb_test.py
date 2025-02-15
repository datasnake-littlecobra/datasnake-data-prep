import os
from datetime import datetime
import polars as pl
import duckdb
from deltalake import DeltaTable, write_deltalake
import logging

access_key = os.getenv("AWS_ACCESS_KEY")
secret_key = os.getenv("AWS_SECRET_KEY")
hostname = "sjc1.vultrobjects.com"

bucket_name = "deltalake-gadm"
delta_s3_key_raw = "gadm0"
raw_bucket_uri= f"s3://{bucket_name}/{delta_s3_key_raw}"

storage_options = {
    "AWS_ACCESS_KEY_ID": access_key,
    "AWS_SECRET_ACCESS_KEY": secret_key,
    "AWS_ENDPOINT": f"https://{hostname}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}

# Load raw Delta Lake data into DuckDB
con = duckdb.connect()
con.sql(f"INSTALL delta; LOAD delta;")

con.query(
        """
               INSTALL httpfs;
               LOAD httpfs;
                CREATE SECRET secretaws (
                TYPE S3,
                KEY_ID {access_key},
                SECRET {secret_key}
            );
               """
    )
con.query(
        f"""
                        SELECT count(*)
                        FROM delta_scan('{raw_bucket_uri}')
                        where year=2010
                    """
    )

print("here4")
    