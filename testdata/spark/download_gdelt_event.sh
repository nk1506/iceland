#!/bin/bash

# Check if a date is provided as a parameter, otherwise terminate
if [ -z "$1" ]; then
    echo "No date provided. Terminating the script."
    exit 1
else
    DATE=$1
    echo "Using provided date argument: $DATE"
fi

# Base URL for downloading the file
BASE_URL="http://data.gdeltproject.org/events"
FILE_URL="$BASE_URL/$DATE.export.CSV.zip"

# Download the file
echo "Downloading file from $FILE_URL..."
wget -O "$DATE.export.CSV.zip" $FILE_URL

# Check if the download was successful
if [ $? -eq 0 ]; then
    echo "File downloaded successfully: $DATE.export.CSV.zip"
else
    echo "Failed to download the file. Please check the URL and try again."
    exit 1
fi

# Unzip the downloaded file
echo "Unzipping the file..."
unzip "$DATE.export.CSV.zip"

# Check if the unzip was successful
if [ $? -eq 0 ]; then
    echo "File unzipped successfully."
else
    echo "Failed to unzip the file."
    exit 1
fi
