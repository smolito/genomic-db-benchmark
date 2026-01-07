#!/bin/bash

# fancy colors
GREEN='\033[0;32m'
NC='\033[0m'

echo -e "${GREEN}Running Benchmark with JSON config...${NC}"

# Zkontrolujeme, jestli kontejner běží (pro jistotu)
if [ ! "$(docker ps -q -f name=genomic-postgres)" ]; then
    echo "Container is not running. Starting it..."
    docker-compose up -d
    echo "Waiting 5s for DB wakeup..."
    sleep 5
fi

# Spuštění benchmarku s configem
# --config: cesta k JSONu s definicí dotazů
# --output: název výstupního souboru (přidáváme časové razítko)
# --iterations: počet opakování (50 je rozumný střed)

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CONFIG_FILE="queries/test_BD.json"

python3 src/benchmark.py \
    --config "$CONFIG_FILE" \
    --output "results_test_${TIMESTAMP}.csv"

echo -e "${GREEN}Benchmark finished! Results saved to results_chr17_${TIMESTAMP}.csv${NC}"