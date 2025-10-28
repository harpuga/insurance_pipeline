#!/bin/bash
set -e

echo "🚀 Starting Streamlit Dashboard..."
echo "📊 Dashboard will be available at: http://localhost:8501"

# Check if database exists
if [ ! -f "data/db/warehouse.duckdb" ]; then
    echo "❌ Database not found. Please run the pipeline first:"
    echo "   docker run --rm -v \$(pwd)/data:/app/data insurance-pipeline"
    exit 1
fi

echo "✅ Database found. Starting dashboard..."
streamlit run dashboard.py --server.port=8501 --server.address=0.0.0.0
