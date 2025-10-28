select
            payment_id,
            policy_id,
            cast(payment_date as date) as payment_date,
            cast(amount as numeric(12,2)) as amount,
            case
              when lower(payment_method) in ('card', 'credit card', 'credit_card', 'cc') then 'CARD'
              when lower(payment_method) in ('ach', 'ach transfer', 'bank_transfer') then 'ACH'
              when lower(payment_method) in ('check', 'cheque', 'chk') then 'CHECK'
              else upper(payment_method)
            end as payment_method

from        read_parquet('/app/data/raw/payments.parquet')