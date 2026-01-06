#!/bin/bash

# fancy colors in terminal
GREEN='\033[0;32m'
NC='\033[0m'

source .venv/bin/activate

echo -e "${GREEN}[1/4] Checking available data...${NC}"

# data URL
DATA_URL="https://bigdataexercise01.blob.core.windows.net/bigdata-exercises-container/merged_samples.vcf.gz"
DATA_DIR="./data"
FILE_NAME="merged_samples.vcf.gz"

# creates dir if it doesn't exist
mkdir -p $DATA_DIR


# check for file
if [ -f "$DATA_DIR/$FILE_NAME" ]; then
    echo "File $FILE_NAME already exists. Skipping the download."
else
    echo "Downloading data from $DATA_URL..."
    # use either curl or wget
    if command -v curl &> /dev/null; then
        curl -L -o "$DATA_DIR/$FILE_NAME" "$DATA_URL"
    elif command -v wget &> /dev/null; then
        wget -O "$DATA_DIR/$FILE_NAME" "$DATA_URL"
    else
        echo "Error: Neither curl nor wget is available. Download manually."
        exit 1
    fi
fi

echo -e "${GREEN}[2/4] Launching the database from docker-compose...${NC}"
# force recreate to ensure clean state with new schema
docker-compose up -d

echo -e "${GREEN}[3/4] Installing python dependencies...${NC}"
python3 -m pip install -r requirements.txt

echo -e "${GREEN}[4/4] Running ETL process (Loading data)...${NC}"
# wait for postgres to be ready
echo "Waiting 10s for PostgreSQL to initialize..."
sleep 10
python3 src/etl.py

if [ $? -eq 0 ]; then
    echo -e "${GREEN}ETL Success!${NC}"
else
    echo "ETL Failed! Check logs."
    exit 1
fi

# echo -e "${GREEN}[5/4] Running Benchmark...${NC}"

# hardcoded queries
# python3 src/benchmark.py --iterations 50 --warmup 5 --output results_$(date +%Y%m%d_%H%M%S).csv

echo -e "${GREEN}Hotovo! All done. Ready to run_benchmark.sh.${NC}"
