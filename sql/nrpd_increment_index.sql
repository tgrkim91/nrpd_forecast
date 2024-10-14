drop table if exists #attribution_v3_signups;
select driver_id,
       signup_date,
       signup_month,
       case when country='AU' and channel_lvl_5 in ('Google_Desktop','Google_Mobile') then 'Google'
            when country='AU' and channel_lvl_5 in ('Google_Desktop_Brand','Google_Mobile_Brand') then 'Google_Brand'
            else channel_lvl_5 end as channels,
       platform,
       country,
       2 as pick_rank
into #attribution_v3_signups
from marketing.marketing_channel_by_driver
where country in ('US')
and channel_lvl_5<>'Unknown';


drop table if exists #data_warehouse_signups;
select ddi.driver_id,
       dd.date as signup_date,
       date_trunc('month', dd.date)::d as signup_month,
       cd.channel_name as channels,
       sf.platform,
       ld.country_code as country
into #data_warehouse_signups
from warehouse.session_fact sf
join warehouse.date_dim dd
    on sf.session_date_pdt = dd.date
join warehouse.location_dim ld
    on sf.source_location_key = ld.location_key
join warehouse.channel_dim cd
    on sf.channel_key = cd.channel_key
join warehouse.sign_up_conversion_mart sucf
    on sf.session_key = sucf.session_key
join warehouse.driver_dim ddi
    on sucf.driver_key=ddi.driver_key
where true
    and sucf.rank_desc_paid_90_days = 1
;

drop table if exists #data_warehouse_signups_pick;
select *,
       1 as pick_rank
into #data_warehouse_signups_pick
from #data_warehouse_signups
where 1=1
    and country = 'US'
    and channels = 'Apple_Brand'
;

drop table if exists #temp_signup_base;
select *
into #temp_signup_base
from
    (select *,
           row_number() over (partition by driver_id order by pick_rank) as rank1
    from
        (select *
            from #attribution_v3_signups
        union all
            (select * from #data_warehouse_signups_pick)))
where rank1=1;

------------------------------------------------------------------------------------------------------------------------
-- NRPD by increments_from_signup
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #nrpd_increment;
SELECT a.forecast_month
     , b.increments_from_signup
     , SUM(b.paid_day)                                                                                                     AS paid_days
     , SUM(b.net_revenue_usd)                                                                                              AS net_revenue_usd
INTO #nrpd_increment
FROM 
    (SELECT distinct signup_month as forecast_month
    FROM #temp_signup_base
    WHERE forecast_month >= '2022-01-01') as a
LEFT JOIN 
    (SELECT s.signup_month
        , FLOOR(DATEDIFF(day, s.signup_date, d.date::date)/30) + 1                                                           AS increments_from_signup
        , DATEADD('month', CAST(increments_from_signup - 1 AS int), s.signup_month)                                          AS trip_month
        , CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) THEN 1 ELSE 0 END                             AS paid_day
        , rps.gaap_net_revenue / rd.paid_days::FLOAT                                                                    AS net_revenue_usd
    FROM finance.reservation_profit_summary_staging rps
            JOIN analytics.reservation_summary rs
                ON rps.reservation_id = rs.reservation_id
            JOIN (SELECT *,
                CASE WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.01 THEN 'A1'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.0045 THEN 'A1'
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.02 THEN 'A2'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.009 THEN 'A2'
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.03 THEN 'A3'   
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.0135 THEN 'A3' 
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.06 THEN 'B'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.0315 THEN 'B'       
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.09 THEN 'C'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.05 THEN 'C'       
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.12 THEN 'D'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.07 THEN 'D'       
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.18 THEN 'E'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.10 THEN 'E'    
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco >= 0.18 THEN 'F'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco > 0.10 THEN 'F'       
                        WHEN monaco IS NULL THEN 'NA'
                    ELSE 'NA' END as monaco_bin
                FROM analytics.reservation_dimensions) rd
                ON rps.reservation_id = rd.reservation_id
            JOIN #temp_signup_base s
                ON rs.driver_id = s.driver_id
            JOIN analytics.date d
                ON d.date BETWEEN COALESCE(rs.trip_start_ts, rd.current_start_ts)::D AND DATEADD('day', rd.paid_days::INT - 1, COALESCE(rs.trip_start_ts, rs.current_start_ts))::D
    WHERE TRUE
        -- drop reservations where trip dates were before their signup date (some errors in the observations)
        AND s.signup_month <= d.date) as b
ON b.trip_month < a.forecast_month -- to ensure that we are observing trip days that have actually occured till the month we are interested in
    AND b.trip_month >= DATEADD('month', -12, a.forecast_month)
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
-- NRPD by (trip_month)
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #nrpd;
SELECT a.forecast_month
     , SUM(b.paid_day)                                                                                                     AS paid_days
     , SUM(b.net_revenue_usd)                                                                                              AS net_revenue_usd
INTO #nrpd
FROM 
    (SELECT distinct signup_month as forecast_month
    FROM #temp_signup_base
    WHERE forecast_month >= '2022-01-01') as a
LEFT JOIN 
    (SELECT s.signup_month
        , FLOOR(DATEDIFF(day, s.signup_date, d.date::date)/30) + 1                                                           AS increments_from_signup
        , DATEADD('month', CAST(increments_from_signup - 1 AS int), s.signup_month)                                          AS trip_month
        , CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) THEN 1 ELSE 0 END                             AS paid_day
        , rps.gaap_net_revenue / rd.paid_days::FLOAT                                                                    AS net_revenue_usd
    FROM finance.reservation_profit_summary_staging rps
            JOIN analytics.reservation_summary rs
                ON rps.reservation_id = rs.reservation_id
            JOIN (SELECT *,
                CASE WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.01 THEN 'A1'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.0045 THEN 'A1'
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.02 THEN 'A2'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.009 THEN 'A2'
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.03 THEN 'A3'   
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.0135 THEN 'A3' 
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.06 THEN 'B'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.0315 THEN 'B'       
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.09 THEN 'C'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.05 THEN 'C'       
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.12 THEN 'D'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.07 THEN 'D'       
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco < 0.18 THEN 'E'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco <= 0.10 THEN 'E'    
                        WHEN created::TIMESTAMP < '2023-04-19 18:28:00' AND monaco >= 0.18 THEN 'F'
                        WHEN created::TIMESTAMP >= '2023-04-19 18:28:00' AND monaco > 0.10 THEN 'F'       
                        WHEN monaco IS NULL THEN 'NA'
                    ELSE 'NA' END as monaco_bin
                FROM analytics.reservation_dimensions) rd
                ON rps.reservation_id = rd.reservation_id
            JOIN #temp_signup_base s
                ON rs.driver_id = s.driver_id
            JOIN analytics.date d
                ON d.date BETWEEN COALESCE(rs.trip_start_ts, rd.current_start_ts)::D AND DATEADD('day', rd.paid_days::INT - 1, COALESCE(rs.trip_start_ts, rs.current_start_ts))::D
    WHERE TRUE
        -- drop reservations where trip dates were before their signup date (some errors in the observations)
        AND s.signup_month <= d.date) as b
ON b.trip_month < a.forecast_month -- to ensure that we are observing trip days that have actually occured till the month we are interested in
    AND b.trip_month >= DATEADD('month', -12, a.forecast_month)
GROUP BY 1;

SELECT a.forecast_month
    , a.increments_from_signup
    , a.net_revenue_usd/a.paid_days as nrpd_increment
    , b.net_revenue_usd/b.paid_days as nrpd_all
from #nrpd_increment as a
left join #nrpd as b on a.forecast_month = b.forecast_month
order by 1, 2;