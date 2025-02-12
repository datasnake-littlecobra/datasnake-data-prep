#!/bin/bash
echo "Before Using Python at: $(which python3.12)"
# Define Python version explicitly
PYTHON_BIN="/usr/bin/python3.12"  # Update to the correct path for your Python version
PIP_BIN="$PYTHON_BIN -m pip"

# Define project directory and venv location
PROJECT_DIR="/home/dev/datasnake-data-prep"
VENV_DIR="$PROJECT_DIR/venv"

# Upgrade pip
# echo "Upgrading pip..."
# $VENV_DIR/bin/python -m pip install --upgrade pip

# get inside the project directory
echo "Going inside the project dir..."
cd $PROJECT_DIR

# Activate venv
# python3 -m venv venv
# source venv/bin/activate
# Create a virtual environment if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    $PYTHON_BIN -m venv "$VENV_DIR"
fi

# Activate the virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"
echo "After Using Python at: $(which python3.12)"

# Step 3.5: Install the required dependencies (from requirements.txt)
if [ -f "/home/dev/datasnake-data-prep/requirements.txt" ]; then
    pip install --upgrade pip setuptools
    $VENV_DIR/bin/python -m pip install -r "$PROJECT_DIR/requirements.txt"
    # pip3 install -r /home/dev/usgs-earthquake-data-pipeline/requirements.txt
fi

# # Verify Polars installation
# if ! $VENV_DIR/bin/python -c "import polars" &>/dev/null; then
#     echo "Polars is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install polars
# fi


# # Verify geopy installation
# # if ! python3 -c "import geopy" &>/dev/null; then
# #     echo "geopy is not installed. Installing it explicitly..."
# #     pip3 install geopy
# # fi

# # Verify duckdb installation
# if ! $VENV_DIR/bin/python -c "import duckdb" &>/dev/null; then
#     echo "duckdb is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install duckdb
# fi

# # Verify relativedelta installation
# if ! $VENV_DIR/bin/python -c "import relativedelta" &>/dev/null; then
#     echo "relativedelta is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install relativedelta
# fi

# # Verify Polars installation
# if ! $VENV_DIR/bin/python -c "import cassandra-driver" &>/dev/null; then
#     echo "cassandra-driver is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install cassandra-driver
# fi

# # Verify boto3 installation
# if ! $VENV_DIR/bin/python -c "import boto3" &>/dev/null; then
#     echo "boto3 is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install boto3
# fi

# # Verify geojson installation
# if ! $VENV_DIR/bin/python -c "import geojson" &>/dev/null; then
#     echo "geojson is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install geojson
# fi

# if ! $VENV_DIR/bin/python -c "import cassandra-driver" &>/dev/null; then
#     echo "cassandra-driver is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install cassandra-driver
# fi

# if ! $VENV_DIR/bin/python -c "import hvac" &>/dev/null; then
#     echo "hvac is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install hvac
# fi

# # if ! python3 -c "import requests" &>/dev/null; then
# #     echo "requests is not installed. Installing it explicitly..."
# #     pip3 install requests
# # fi

# if ! $VENV_DIR/bin/python -c "import deltalake" &>/dev/null; then
#     echo "deltalake is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install deltalake
# fi

# if ! $VENV_DIR/bin/python -c "import pyarrow" &>/dev/null; then
#     echo "pyarrow is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install pyarrow
# fi

# if ! $VENV_DIR/bin/python -c "import packaging" &>/dev/null; then
#     echo "packaging is not installed. Installing it explicitly..."
#     $VENV_DIR/bin/python -m pip install packaging
# fi

CASSANDRA_HOST="127.0.0.1"
USERNAME="cassandra"
PASSWORD="cassandra"
PROJECT_NAME="datasnake-data-prep"
CQL_FILE="/home/dev/${PROJECT_NAME}/db-script.cql"
echo $CQL_FILE
cqlsh $CASSANDRA_HOST -u $USERNAME -p $PASSWORD -f $CQL_FILE


# Step 3.7: S3 Bucket Creation (Dynamic)
# project_name="datasnake-data-prep"
bucket_name_gadm="deltalake-gadm"
bucket_uri_gadm="s3://$bucket_name_gadm"

# Check if the bucket exists
if ! s3cmd ls | grep -q "$bucket_uri_gadm"; then
    echo "Bucket does not exist. Creating delta lake bucket: $bucket_uri_gadm"
    s3cmd mb "$bucket_uri_gadm"
else
    echo "Bucket $bucket_uri_gadm already exists."
fi

bucket_name_wof="deltalake-wof"
bucket_uri_wof="s3://$bucket_name_wof"

# Check if the bucket exists
if ! s3cmd ls | grep -q "$bucket_uri_wof"; then
    echo "Bucket does not exist. Creating delta lake bucket: $bucket_uri_wof"
    s3cmd mb "$bucket_uri_wof"
else
    echo "Bucket $bucket_uri_wof already exists."
fi

# Initiating running main code
echo "starting to run datasnake_data_prep py file!"

# Activate the virtual environment
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Confirm the environment by checking Python executable
echo "Using Python at to run code: $(which python3.12)"
echo "Pip list before running code: $($PIP_BIN list)"

# Step 3.6: Run your Python script or entry point (e.g., main.py)
$VENV_DIR/bin/python /home/dev/datasnake-data-prep/datasnake_data_prep.py
