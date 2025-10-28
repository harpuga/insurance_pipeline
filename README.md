# Insurance Data Pipeline

A data pipeline that processes insurance CSV files through Python ETL and dbt transformations, creating a DuckDB database for analysis.

## 🚀 How to Run

### Prerequisites
- Docker
- DBeaver (for database exploration)

### Quick Start

#### Pipeline Only
```bash
# Build and run the complete pipeline
docker build -t insurance-pipeline .
docker run --rm -v $(pwd)/data:/app/data insurance-pipeline
```

#### Pipeline + Interactive Dashboard (Recommended)
```bash
# Step 1: Run pipeline first
docker build -t insurance-pipeline .
docker run --rm -v $(pwd)/data:/app/data insurance-pipeline

# Step 2: Run dashboard separately (use different port if 8501 is busy)
docker run --rm -p 8502:8501 -v $(pwd)/data:/app/data --entrypoint="" insurance-pipeline bash -c "cd /app && streamlit run dashboard.py --server.port=8501 --server.address=0.0.0.0"
```
**Dashboard**: http://localhost:8502


### Database Access
After execution, connect DBeaver to `data/db/warehouse.duckdb` to explore the processed data.

**Available Tables:**
- Staging: `stg_agents`, `stg_endorsements`, `stg_payments`, `stg_policies`
- DWH: `policy_cash_collections`, `policy_premium_transactions`
- Marts: `f_policy_metrics`

## 📋 Assumptions

- Docker runs in Mac/Unix/Windows environment
- All dates are in UTC timezone
- Basis points (bps) = 0.01% for commission rates
- Input data is clean (no duplicates, valid references)

## 🔍 Data Quality Checks

- **Uniqueness**: Primary keys (policy_id, endorsement_id, payment_id)
- **Referential Integrity**: Foreign key relationships (endorsements→policies, payments→policies, policies→agents)
- **Business Rules**: Non-negative amounts and premiums
- **Date Validation**: Valid date formats, reasonable date ranges, logical date relationships (inception < expiration)

Results saved to `data/reports/raw_dq_summary.csv`

## 🚀 Improvements with More Time

### Architecture
- [ ] Separate schemas for each layer (stage/dwh/mart)
- [ ] Incremental data processing

### Data Quality
- [ ] Enhanced DQA logging with detailed failure info
- [ ] Data lineage documentation

### Operations
- [ ] Slack alerting for pipeline failures
- [ ] Automated testing framework

### Development
- [ ] Poetry dependency management

## 🐛 Troubleshooting

### Dashboard Not Loading
1. **Wait for pipeline completion**: The dashboard only starts after the pipeline finishes
2. **Check container status**: Run `docker ps` to see if container is running
3. **Try different port**: Use `-p 8502:8501` if port 8501 is busy
4. **Check logs**: Remove `--rm` flag to inspect container logs

### Common Issues
- **"Port already in use"**: Change port mapping to `-p 8502:8501`
- **"Database not found"**: Ensure pipeline completed successfully first
- **"Permission denied"**: Scripts are made executable in Dockerfile
