#!/bin/bash

DATA_DIR="../data/"
COMPLAINTS_HFS_PATH="/user/jr4964/final-project/complaints.csv"
COMPLAINTS_NAME="complaints_python_script.csv"
ZIP_HFS_PATH="/user/djk525/big-data/project/data/zip_zcta.csv"
ZIP_NAME="zip_zcta_python_script.csv"
OUTPUT_NAME="311_reduced_python_script.csv"

# Check if the local files exist, otherwise get them.
if [[ ! $DATA_DIR$COMPLAINTS_NAME ]]; then
    /usr/bin/hadoop fs -getmerge $COMPLAINTS_HFS_PATH $DATA_DIR$COMPLAINTS_NAME
fi

if [[ ! $DATA_DIR$ZIP_NAME ]]; then
    !/usr/bin/hadoop fs -getmerge $ZIP_HFS_PATH $DATA_DIR$ZIP_NAME
fi

python3 prepare_311_data_python_script.py $DATA_DIR$COMPLAINTS_NAME $DATA_DIR$ZIP_NAME $DATA_DIR$OUTPUT_NAME
