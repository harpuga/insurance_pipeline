-- q1 - Top 5 agents by net written premium
with agent_base as 
(
select 		
			agent_id, 
			agent_name,  
			sum(net_written_premium) as net_written_premium 
from 		f_policy_metrics 
group by 	all
)
select 		
			agent_id, 
			agent_name, 
			net_written_premium, 
			rank() over (order by net_written_premium desc) as top_n
from 		agent_base 
qualify 	rank() over (order by net_written_premium desc)  <= 5
;

-- q2 - Collected premium totals by line_of_business
select 		
			line_of_business,  
			sum(collected_premium_total) as collected_premium_total
from 		f_policy_metrics 
group by 	line_of_business
order by    collected_premium_total desc
;

-- q3 - Policies with a cancel + reinstate endorsement (policy_id and dates).
select 		policy_id, 
			inception_date, 
			expiration_date
from   		policy_premium_transactions 
where 		endorsement_delta_cancel <> 0 
and 		endorsement_delta_reinstate <> 0
;