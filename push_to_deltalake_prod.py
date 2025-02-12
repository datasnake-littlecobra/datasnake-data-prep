# save_to_delta.py
import os
from datetime import datetime
import polars as pl
from deltalake import DeltaTable, write_deltalake
import boto3
import hvac
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log"), logging.StreamHandler()],
)


# client = hvac.Client(url="http://127.0.0.1:8200", token="")
client = hvac.Client(url="http://127.0.0.1:8200")
# print("client.is_authenticated() : ")
# print(client.is_authenticated())

datasnake_test_role_id = os.environ["DATASNAKE_TEST_ROLE_ID"]
datasnake_test_secret_id = os.environ["DATASNAKE_TEST_SECRET_ID"]

response = client.auth.approle.login(
    role_id=datasnake_test_role_id, secret_id=datasnake_test_secret_id
)
client.token = response["auth"]["client_token"]
# print(client.token)
# print(response)


# Access the secret
secret_path_v1 = "secret/s3keys"
# For KV version 2, use 'data/' in the path
secret_path_v2 = "secret/data/s3keys"
# using this path
secret_path = "s3keys"

# read secret using secret_path_v1 with NO default key path set to "secret" by VAULT
# print(client.read(secret_path_v1))
# vault_response = client.read(secret_path_v1)

# using v1 kv with default path SET TO = "secret" by VAULT
secret_response = client.secrets.kv.v1.read_secret(
    path=secret_path, mount_point="secret"
)

# Print the secret values
# access_key = vault_response["data"]["data"]["aws_access_key_id"]
# secret_key = vault_response["data"]["data"]["aws_secret_access_key"]

access_key = secret_response["data"]["aws_access_key_id"]
secret_key = secret_response["data"]["aws_secret_access_key"]

# print(f"Access Key: {access_key}")
# print(f"Secret Key: {secret_key}")

hostname = "sjc1.vultrobjects.com"

# Connect to the Cassandra cluster
# USERNAME = "cassandra"
# PASSWORD = "cassandra"
# auth_provider = PlainTextAuthProvider(USERNAME, PASSWORD)
# cluster = Cluster(
#     ["127.0.0.1"], auth_provider=auth_provider
# )  # Replace with container's IP if needed
# session = cluster.connect()


# # Use the keyspace
# session.set_keyspace("test_keyspace")

gadm_delta_s3_bucket_raw_prod = f"gadm"
gadm_delta_s3_key_raw_prod = f"gadm"
gadm_s3_uri = f"s3://{gadm_delta_s3_bucket_raw_prod}/{gadm_delta_s3_key_raw_prod}"


storage_options = {
    "AWS_ACCESS_KEY_ID": access_key,
    "AWS_SECRET_ACCESS_KEY": secret_key,
    "AWS_ENDPOINT": f"https://{hostname}",
    "AWS_REGION": "us-east-1",
    "AWS_S3_ADDRESSING_STYLE": "path",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
}


# Function to save data to Delta Lake format
def save_to_deltalake_local(data: pl.DataFrame, path: str, mode):
    try:
        # logging.info(f"inside save to delta table")
        # Ensure the path exists, or create it (you could use pathlib for this)
        os.makedirs(path, exist_ok=True)

        # Create a file path within the directory
        file_path = os.path.join(path, "")
        # logging.info(f"Starting to write into Delta Parquet: {file_path}")
        # logging.info(datetime.now())
        # Check if the table exists and handle mode appropriately
        if os.path.exists(file_path):
            if mode == "overwrite":
                # logging.info(f"Overwriting the existing Delta Lake table.")
                # data.write_delta(file_path,mode="overwrite")
                # data.write_delta(target=path, delta_write_options={"year", "month"}, mode="append")
                write_deltalake(
                    table_or_uri=path,
                    data=data.to_arrow(),  # Convert Polars DataFrame to Arrow Table
                    mode="overwrite",  # Write mode: "append", "overwrite", etc.
                    partition_by=["country", "state", "city"],  # Specify partition keys
                )
                # logging.info(
                #     f"Data successfully written to {file_path} in {mode} mode."
                # )
            elif mode == "append":
                # logging.info(f"Appending to the existing Delta Lake table.")
                # data.write_delta(file_path,mode="append")
                write_deltalake(
                    table_or_uri=path,
                    data=data.to_arrow(),  # Convert Polars DataFrame to Arrow Table
                    mode="append",  # Write mode: "append", "overwrite", etc.
                    partition_by=["country", "state", "city"],  # Specify partition keys
                )
                return
            else:
                raise ValueError("Invalid mode: Choose either 'overwrite' or 'append'.")
        else:
            logging.info(f"Creating a new Delta Lake table.")
    except Exception as e:
        logging.error(f"Errored out in save_to_delta_table function: {e}")
        raise e


# Upload Delta Table to S3
def upload_raw_delta_to_s3_prod(
    dataframe: pl.DataFrame, uri_path: str, partition_by: list
):
    try:
        print("inside upload raw deltalake to s3 prod: ")
        print("uri path value:", uri_path)
        print("partition_by value:", partition_by)
        write_deltalake(
            table_or_uri=uri_path,
            storage_options=storage_options,
            data=dataframe.to_arrow(),  # Convert Polars DataFrame to Arrow Table
            mode="overwrite",  # Write mode: "append", "overwrite", etc.
            partition_by=partition_by,  # Specify partition keys
        )
    except Exception as e:
        logging.error(f"Error uploading Delta table to S3: {e}")


# Upload Delta Table to S3
# def upload_delta_to_s3(delta_path, bucket, key):
#     try:
#         # Upload all files in the Delta table directory to S3
#         for root, dirs, files in os.walk(delta_path):
#             for file in files:
#                 local_file = os.path.join(root, file)
#                 s3_key = os.path.join(key, os.path.relpath(local_file, delta_path))
#                 s3_client.upload_file(local_file, bucket, s3_key)
#                 # logging.info(f"Uploaded {local_file} to S3 at {s3_key}")
#     except Exception as e:
#         logging.error(f"Error uploading Delta table to S3: {e}")
