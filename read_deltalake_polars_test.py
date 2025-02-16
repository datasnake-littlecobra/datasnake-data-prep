import os
import polars as pl
import logging
import time

access_key = os.getenv("AWS_ACCESS_KEY")
secret_key = os.getenv("AWS_SECRET_KEY")
hostname = "sjc1.vultrobjects.com"

storage_options = {
    "AWS_ACCESS_KEY_ID": access_key,
    "AWS_SECRET_ACCESS_KEY": secret_key,
    "AWS_ENDPOINT_URL": f"https://{hostname}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
}
# Delta table options
delta_table_options = {
    "ignoreDeletes": "true",  # Ignore delete operations
    "ignoreChanges": "true",  # Ignore incremental changes
}

# PyArrow options (optional, applied when `use_pyarrow=True`)
pyarrow_options = {
    "use_threads": True,  # Enable multi-threading for performance
    "coerce_int96_timestamp_unit": "ms",  # Adjust timestamp precision
}

bucket_name = "deltalake-gadm"
delta_s3_key_raw_gadm0 = "gadm0"
delta_s3_key_raw_gadm1 = "gadm1"
delta_s3_key_raw_gadm2 = "gadm2"
raw_bucket_uri_gadm0 = f"s3://{bucket_name}/{delta_s3_key_raw_gadm0}"
raw_bucket_uri_gadm1 = f"s3://{bucket_name}/{delta_s3_key_raw_gadm1}"
raw_bucket_uri_gadm2 = f"s3://{bucket_name}/{delta_s3_key_raw_gadm2}"


def read_delta_lake_using_polars():
    try:
        # Load Delta Table locally
        logging.info(f"bucket uri: {raw_bucket_uri_gadm0}")
        print(f"bucket uri: {raw_bucket_uri_gadm0}")
        #         df = pl.read_delta(
        #             raw_bucket_uri, storage_options=storage_options,
        #             columns=["country_code","country_full_name"],
        # #            pyarrow_options=pyarrow_options,
        # #            use_pyarrow=True
        #         )
        start_time = time.time()  # Start timing
        df = pl.scan_delta(raw_bucket_uri_gadm0, storage_options=storage_options).collect()
        end_time = time.time()
        time_taken = end_time - start_time
        row_count = df.shape[0]
        df_size_mb = df.estimated_size() / (1024 * 1024)  # Convert bytes to MB
        print(
            f"Processed {len(df)} | {row_count} rows, {df_size_mb:.2f} MB in {time_taken:.2f} sec"
        )


        start_time = time.time()  # Start timing
        df = pl.scan_delta(raw_bucket_uri_gadm0, storage_options=storage_options).select(
            ["country_code"])
        end_time = time.time()
        time_taken = end_time - start_time
        row_count = df.shape[0]
        df_size_mb = df.estimated_size() / (1024 * 1024)  # Convert bytes to MB
        print(
            f"Processed {len(df)} | {row_count} rows, {df_size_mb:.2f} MB in {time_taken:.2f} sec"
        )
        df_size_mb = df.estimated_size() / (1024 * 1024)  # Convert bytes to MB

        print(df.head())
    except Exception as e:
        raise e
