with payments as
(
select 	
            policy_id, 
            sum(amount) as collected_premium_total
from 		{{ ref('stg_payments') }} 
group by 	policy_id
)
select
            pt.policy_id,
            pt.line_of_business,
            pt.insured_name,
            pt.inception_date,
            pt.expiration_date,
            pt.status,
            pt.written_premium_base,
            pt.endorsement_delta_total,
            coalesce(pt.written_premium_base, 0) + coalesce(pt.endorsement_delta_total, 0) as net_written_premium,
            coalesce(pay.collected_premium_total, 0) as collected_premium_total,
            pt.agent_id,
            a.agent_name

from        {{ ref('policy_premium_transactions') }} pt
            left join {{ ref('stg_agents') }} a 
              using (agent_id)
            left join payments pay
              on pt.policy_id = pay.policy_id
