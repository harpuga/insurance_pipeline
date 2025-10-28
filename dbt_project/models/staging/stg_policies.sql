select
          policy_id,
          insured_name,
          cast(inception_date as date) as inception_date,
          cast(expiration_date as date) as expiration_date,
          upper(line_of_business) as line_of_business,
          written_premium,
          upper(status) as status,
          agent_id
from      read_parquet('/app/data/raw/policies.parquet')