select
            endorsement_id,
            policy_id,
            cast(premium_delta as numeric(12,2)) as premium_delta,
            cast(effective_date as date) as effective_date,
            upper(transaction_type) as transaction_type

from        read_parquet('/app/data/raw/endorsements.parquet')