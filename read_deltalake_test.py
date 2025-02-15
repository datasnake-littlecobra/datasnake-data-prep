import polars as pl
import hvac
import logging

client = hvac.Client(url="http://127.0.0.1:8200")
# Access the secret
secret_path_v1 = "secret/s3keys"
# For KV version 2, use 'data/' in the path
secret_path_v2 = "secret/data/s3keys"
# using this path
secret_path = "s3keys"
# using v1 kv with default path SET TO = "secret" by VAULT
# works
secret_response = client.secrets.kv.v1.read_secret(
    path=secret_path, mount_point="secret"
)

access_key = secret_response["data"]["aws_access_key_id"]
secret_key = secret_response["data"]["aws_secret_access_key"]
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
delta_s3_key_raw = "gadm0"
raw_bucket_uri= f"s3://{bucket_name}/{delta_s3_key_raw}"

def read_delta_lake_using_polars():
    try:
        # Load Delta Table locally
        logging.info(f"bucket uri: {raw_bucket_uri}")
        print("bucket uri: {raw_bucket_uri}")
        df = pl.read_delta(
            raw_bucket_uri, storage_options=storage_options,
            columns=["country_code","country_full_name"],
#            pyarrow_options=pyarrow_options,
#            use_pyarrow=True
        )
        print(df.head())
    except Exception as e:
        raise e

