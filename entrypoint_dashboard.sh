#!/bin/bash
set -e

echo "=== [Step 1/2] Running Raw Layer (Python) ==="
python raw_data_layer.py

echo "=== [Step 2/2] Running Stage, DWH and Marts Layers (dbt) ==="
cd dbt_project
dbt deps
dbt build

echo "=== [Step 3/3] Starting Dashboard ==="
cd ..
echo "🚀 Starting Streamlit Dashboard..."
echo "📊 Dashboard will be available at: http://localhost:8501 or http://localhost:8502"
streamlit run dashboard.py --server.port=8501 --server.address=0.0.0.0
