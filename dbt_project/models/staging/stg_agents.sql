select
            agent_id as agent_id,
            agent_name as agent_name,
            cast(commission_rate_bps as integer) as commission_rate_bps,
            commission_rate_bps/10000.0 as commission_rate --bps to percentage

from        read_parquet('/app/data/raw/agents.parquet')
