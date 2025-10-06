#!/bin/bash
INPUT_DIR="/data"
for fgbfile in "$INPUT_DIR"/*.fgb; do
    if [ -f "$fgbfile" ]; then
        parquetfile="${fgbfile%.fgb}.parquet"
        ogr2ogr -f "Parquet" "$parquetfile" "$fgbfile"
        #ogr2ogr -f "Parquet" "$parquetfile" "$fgbfile" -sql "SELECT * EXCLUDE (geometry)" 

    fi
done
