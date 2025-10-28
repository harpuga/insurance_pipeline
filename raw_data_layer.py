import pandas as pd
import os

# ========== CONFIG ==========
INPUT_DIR = "data/input/"           # where your CSVs are stored
RAW_DIR = "data/raw"
REPORT_DIR = "data/reports"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(REPORT_DIR, exist_ok=True)

# ========== LOAD DATA ==========
policies = pd.read_csv(os.path.join(INPUT_DIR, "policies.csv"))
endorsements = pd.read_csv(os.path.join(INPUT_DIR, "endorsements.csv"))
payments = pd.read_csv(os.path.join(INPUT_DIR, "payments.csv"))
agents = pd.read_csv(os.path.join(INPUT_DIR, "agents.csv"))

# ========== DQ CHECKS ==========

summary = []

def check_unique(df, key, name):
    failed = df[key].duplicated().sum()
    summary.append({"dataset": name, "check": f"Unique {key}", "failed_rows": failed, "total_rows": len(df)})

def check_non_negative(df, col, name):
    failed = (df[col] < 0).sum()
    summary.append({"dataset": name, "check": f"Non-negative {col}", "failed_rows": failed, "total_rows": len(df)})

def check_foreign_key(child_df, parent_df, child_col, parent_col, name):
    failed = (~child_df[child_col].isin(parent_df[parent_col])).sum()
    summary.append({"dataset": name, "check": f"Referential integrity {child_col}->{parent_col}", "failed_rows": failed, "total_rows": len(child_df)})

def check_valid_dates(df, date_col, name):
    """Check for invalid dates (null, invalid format, future dates beyond reasonable range)"""
    # Convert to datetime, errors='coerce' will make invalid dates NaT
    df_copy = df.copy()
    df_copy[f'{date_col}_parsed'] = pd.to_datetime(df_copy[date_col], errors='coerce')
    
    # Count invalid dates (NaT values)
    invalid_dates = df_copy[f'{date_col}_parsed'].isna().sum()
    
    # Count future dates beyond 10 years (unreasonable for insurance)
    future_dates = (df_copy[f'{date_col}_parsed'] > pd.Timestamp.now() + pd.DateOffset(years=10)).sum()
    
    # Count dates before 1900 (unreasonable for insurance)
    old_dates = (df_copy[f'{date_col}_parsed'] < pd.Timestamp('1900-01-01')).sum()
    
    total_invalid = invalid_dates + future_dates + old_dates
    
    summary.append({"dataset": name, "check": f"Valid {date_col}", "failed_rows": total_invalid, "total_rows": len(df)})

def check_date_logic(df, start_col, end_col, name):
    """Check that start date is before end date"""
    df_copy = df.copy()
    df_copy[f'{start_col}_parsed'] = pd.to_datetime(df_copy[start_col], errors='coerce')
    df_copy[f'{end_col}_parsed'] = pd.to_datetime(df_copy[end_col], errors='coerce')
    
    # Count where end date is before start date
    invalid_logic = (df_copy[f'{end_col}_parsed'] < df_copy[f'{start_col}_parsed']).sum()
    
    summary.append({"dataset": name, "check": f"Date logic {start_col}<{end_col}", "failed_rows": invalid_logic, "total_rows": len(df)})

# --- Apply checks ---
check_unique(policies, "policy_id", "policies")
check_unique(endorsements, "endorsement_id", "endorsements")
check_unique(payments, "payment_id", "payments")

check_foreign_key(endorsements, policies, "policy_id", "policy_id", "endorsements")
check_foreign_key(payments, policies, "policy_id", "policy_id", "payments")
check_foreign_key(policies, agents, "agent_id", "agent_id", "policies")

if "written_premium" in policies.columns:
    check_non_negative(policies, "written_premium", "policies")

if "amount" in payments.columns:
    check_non_negative(payments, "amount", "payments")

# Date validation checks
check_valid_dates(policies, "inception_date", "policies")
check_valid_dates(policies, "expiration_date", "policies")
check_date_logic(policies, "inception_date", "expiration_date", "policies")

check_valid_dates(endorsements, "effective_date", "endorsements")

check_valid_dates(payments, "payment_date", "payments")

# ========== CLEAN DATA ==========

# Remove duplicates
policies = policies.drop_duplicates(subset=["policy_id"])
endorsements = endorsements.drop_duplicates(subset=["endorsement_id"])
payments = payments.drop_duplicates(subset=["payment_id"])
agents = agents.drop_duplicates(subset=["agent_id"])

# Keep only rows with valid policy references
endorsements = endorsements[endorsements["policy_id"].isin(policies["policy_id"])]
payments = payments[payments["policy_id"].isin(policies["policy_id"])]

# Ensure no negative values
if "written_premium" in policies.columns:
    policies = policies[policies["written_premium"] >= 0]
if "amount" in payments.columns:
    payments = payments[payments["amount"] >= 0]

# Clean invalid dates
policies = policies.dropna(subset=["inception_date", "expiration_date"])
policies = policies[pd.to_datetime(policies["inception_date"], errors='coerce').notna()]
policies = policies[pd.to_datetime(policies["expiration_date"], errors='coerce').notna()]

endorsements = endorsements.dropna(subset=["effective_date"])
endorsements = endorsements[pd.to_datetime(endorsements["effective_date"], errors='coerce').notna()]

payments = payments.dropna(subset=["payment_date"])
payments = payments[pd.to_datetime(payments["payment_date"], errors='coerce').notna()]

# Ensure date logic (inception < expiration)
policies = policies[pd.to_datetime(policies["inception_date"]) < pd.to_datetime(policies["expiration_date"])]

# ========== SAVE CLEANED DATA ==========
policies.to_parquet(os.path.join(RAW_DIR, "policies.parquet"), index=False)
endorsements.to_parquet(os.path.join(RAW_DIR, "endorsements.parquet"), index=False)
payments.to_parquet(os.path.join(RAW_DIR, "payments.parquet"), index=False)
agents.to_parquet(os.path.join(RAW_DIR, "agents.parquet"), index=False)

# ========== SAVE SUMMARY ==========
dq_summary = pd.DataFrame(summary)
dq_summary.to_csv(os.path.join(REPORT_DIR, "raw_dq_summary.csv"), index=False)

print("✅ Data ingestion and validation complete!")
print(dq_summary)
