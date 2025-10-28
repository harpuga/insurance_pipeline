with endorsements as
(
select 	
            policy_id, 
            sum(case when transaction_type = 'CANCEL' then premium_delta end) as endorsement_delta_cancel,
            sum(case when transaction_type = 'INCREASE' then premium_delta end) as endorsement_delta_increase,
            sum(case when transaction_type = 'DECREASE' then premium_delta end) as endorsement_delta_decrease,
            sum(case when transaction_type = 'REINSTATE' then premium_delta end) as endorsement_delta_reinstate,
            sum(premium_delta) as endorsement_delta_total

from 		    {{ ref('stg_endorsements') }} 
group by 	  policy_id
)
select
            p.policy_id,
            p.line_of_business,
            p.insured_name,
            p.inception_date as inception_date,
            p.expiration_date as expiration_date,
            p.status,
            coalesce(p.written_premium, 0) as written_premium_base,
            coalesce(e.endorsement_delta_cancel, 0) as endorsement_delta_cancel,
            coalesce(e.endorsement_delta_increase, 0) as endorsement_delta_increase,
            coalesce(e.endorsement_delta_total, 0) as endorsement_delta_total,
            coalesce(e.endorsement_delta_decrease, 0) as endorsement_delta_decrease,
            coalesce(e.endorsement_delta_reinstate, 0) as endorsement_delta_reinstate,
            coalesce(e.endorsement_delta_total, 0) as endorsement_delta_total,
            p.agent_id

from        {{ ref('stg_policies') }} p
            left join endorsements e
              on p.policy_id = e.policy_id