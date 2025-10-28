select
            policy_id,
            payment_date,
            sum(amount) as total_amount

from        {{ ref('stg_payments') }}

group by    policy_id, 
            payment_date
