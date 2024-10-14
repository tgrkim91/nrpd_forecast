-- NRPD forecast values in each snapshot
SELECT a.scenario
    , b.last_modified_at
    -- last_actual_month indicates the last month of recorded actuals in each forecast snapshot (we cannot use last_modified_at, since it is inconsistent across snapshots)
    , to_date(EXTRACT(YEAR from b.last_modified_at)::TEXT || 
        '-' ||
        substring(a.scenario,
            charindex('(', a.scenario) + 1,
            charindex('+', a.scenario) - charindex('(', a.scenario) - 1), 'YYYY-FMMM') as last_actual_month
    , to_date(a.month, 'Mon-YYYY') as month
    -- 'Other Transaction Revenue' include Insurance and Finco, which are unrelated to trips on the platform
    , sum(case when a.metric = 'Net Revenue (USD $)' and a.revenue_category != 'Other Transaction Revenue' then a.value else 0 end) as net_revenue  
    , sum(case when a.metric = 'GAAP Days (#)' then a.value else 0 end) as trip_days
    , net_revenue/trip_days as NRPD
from analytics.aleph_base a
left join (
    SELECT scenario, max(_aleph_modified_at) as last_modified_at
    from analytics.aleph_base
    group by 1
) b on a.scenario = b.scenario 
where a.country = 'US'
    and a.scenario in (
        'Sep-23 Forecast (8+4)',
        'Oct-23 BOD Forecast (9+3)',
        'Feb-24 BOD Forecast (1+11)',
        'Mar-24 Forecast (2+11)',
        'May-24 BOD Forecast (3+9)',
        'May-24 Forecast (4+8)',
        'Jun-24 Forecast (5+7)',
        'Aug-24 BOD Forecast (6+6)',
        'Sep-24 Forecast (8+4)')
group by 2, 1, 3, 4
order by 2, 1, 3, 4