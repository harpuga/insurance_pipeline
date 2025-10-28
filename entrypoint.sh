#!/bin/bash
set -e

echo "=== [Step 1/2] Running Raw Layer (Python) ==="
python raw_data_layer.py

echo "=== [Step 2/2] Running Stage, DWH and Marts Layers (dbt) ==="
cd dbt_project
dbt deps
dbt build

echo "✅ Pipeline completed successfully!"
