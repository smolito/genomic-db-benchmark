#!/bin/bash

# fancy colors in terminal
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${GREEN}[1/3] Kontrola dat...${NC}"

# data URL
DATA_URL="https://bigdataexercise01.blob.core.windows.net/bigdata-exercises-container/merged_samples.vcf.gz"
DATA_DIR="./data"
FILE_NAME="merged_samples.vcf.gz"

# creates dir if it doesn't exist
mkdir -p $DATA_DIR

# check for file
if [ -f "$DATA_DIR/$FILE_NAME" ]; then
    echo "Soubor $FILE_NAME již existuje. Přeskakuji stahování."
else
    echo "Stahuji data z $DATA_URL..."
    # use either curl or wget
    if command -v curl &> /dev/null; then
        curl -L -o "$DATA_DIR/$FILE_NAME" "$DATA_URL"
    elif command -v wget &> /dev/null; then
        wget -O "$DATA_DIR/$FILE_NAME" "$DATA_URL"
    else
        echo "Chyba: Nemám curl ani wget. Stáhni data ručně."
        exit 1
    fi
fi

echo -e "${GREEN}[2/3] Spouštění databáze...${NC}"
# run docker-compose
docker-compose up -d

echo -e "${GREEN}[3/3] Hotovo!${NC}"
echo "Databáze běží. Nyní můžeš spustit ETL proces."