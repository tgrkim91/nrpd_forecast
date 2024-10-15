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
-- all trips by increment 1 (new signups)
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #trips_increment_1;
SELECT s.signup_month
    , s.channels
    , case when a.platform in ('Android native','Desktop web','iOS native','Mobile web') then a.platform
           else 'Undefined' end as platform
    , FLOOR(DATEDIFF(day, s.signup_date, d.date::date)/30) + 1                                                           AS increments_from_signup
    , DATEADD('month', CAST(increments_from_signup - 1 AS int), s.signup_month)                                          AS trip_month
    , CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) THEN 1 ELSE 0 END                             AS paid_day
    , CASE WHEN rs.is_ever_booked = 1 AND rs.current_status NOT IN (2, 11) THEN 1 ELSE 0 END / rd.paid_days::FLOAT       AS trip
    , rps.gaap_net_revenue / rd.paid_days::FLOAT                                                                    AS net_revenue_usd
INTO #trips_increment_1
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
    LEFT JOIN (select driver_id,max(platform) as platform from marketing.marketing_channel_by_driver group by driver_id) a
        ON rs.driver_id = a.driver_id
WHERE TRUE
    -- drop reservations where trip dates were before their signup date (some errors in the observations)
    AND s.signup_month <= d.date
    AND increments_from_signup = 1;


------------------------------------------------------------------------------------------------------------------------
-- NRPD by segment at increment 1 : segment is used to define net revenue and paid days for data-thin channels
-- Following Jodie's mapping 
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #nrpd_segment;
SELECT *
INTO #nrpd_segment
FROM
    (SELECT a.forecast_month
        , case when b.channels in ('Free_Google','Free_Microsoft','Free_Other') then 'all free' else b.channels end           AS segment
        , SUM(b.paid_day)                                                                                                     AS paid_days
        , SUM(b.net_revenue_usd)                                                                                              AS net_revenue_usd
        -- Track of data volume (i.e. trips) for each channel
        , SUM(b.trip)                                                                                                           AS trips
    FROM 
        (SELECT distinct signup_month as forecast_month
        FROM #temp_signup_base
        WHERE forecast_month >= '2022-01-01') as a
    LEFT JOIN #trips_increment_1 as b 
    ON b.trip_month < a.forecast_month -- to ensure that we are observing trip days that have actually occured till the month we are interested in
        AND b.trip_month >= DATEADD('month', -12, a.forecast_month)
    WHERE b.channels is not null
    GROUP BY 1, 2)
UNION all
    (SELECT a.forecast_month
        , 'all paid'                                                                                                          AS segment
        , SUM(b.paid_day)                                                                                                     AS paid_days
        , SUM(b.net_revenue_usd)                                                                                              AS net_revenue_usd
        , SUM(b.trip)                                                                                                           AS trips
    FROM 
        (SELECT distinct signup_month as forecast_month
        FROM #temp_signup_base
        WHERE forecast_month >= '2022-01-01') as a
    LEFT JOIN #trips_increment_1 as b 
    ON b.trip_month < a.forecast_month 
        AND b.trip_month >= DATEADD('month', -12, a.forecast_month)
    WHERE b.channels not in ('Free_Google','Free_Microsoft','Free_Other')
    GROUP BY 1, 2)
UNION all
    (SELECT a.forecast_month
        , case when b.platform='Desktop web' then 'all desktop'
             when b.platform='Mobile web' then 'all mobile'
             when b.platform='Android native' then 'all android'
             when b.platform='iOS native' then 'all ios' end                                                                  AS segment
        , SUM(b.paid_day)                                                                                                     AS paid_days
        , SUM(b.net_revenue_usd)                                                                                              AS net_revenue_usd
        , SUM(b.trip)                                                                                                           AS trips
    FROM 
        (SELECT distinct signup_month as forecast_month
        FROM #temp_signup_base
        WHERE forecast_month >= '2022-01-01') as a
    LEFT JOIN #trips_increment_1 as b 
    ON b.trip_month < a.forecast_month 
        AND b.trip_month >= DATEADD('month', -12, a.forecast_month)
    GROUP BY 1, 2)
UNION all
    (SELECT a.forecast_month
        , 'all web'                                                                                                           AS segment
        , SUM(b.paid_day)                                                                                                     AS paid_days
        , SUM(b.net_revenue_usd)                                                                                              AS net_revenue_usd
        , SUM(b.trip)                                                                                                           AS trips
    FROM 
        (SELECT distinct signup_month as forecast_month
        FROM #temp_signup_base
        WHERE forecast_month >= '2022-01-01') as a
    LEFT JOIN #trips_increment_1 as b 
    ON b.trip_month < a.forecast_month 
        AND b.trip_month >= DATEADD('month', -12, a.forecast_month)
    WHERE b.platform in ('Desktop web', 'Mobile web')
    GROUP BY 1, 2)
UNION all
    (SELECT a.forecast_month
        , 'all app'                                                                                                           AS segment
        , SUM(b.paid_day)                                                                                                     AS paid_days
        , SUM(b.net_revenue_usd)                                                                                              AS net_revenue_usd
        , SUM(b.trip)                                                                                                           AS trips
    FROM 
        (SELECT distinct signup_month as forecast_month
        FROM #temp_signup_base
        WHERE forecast_month >= '2022-01-01') as a
    LEFT JOIN #trips_increment_1 as b 
    ON b.trip_month < a.forecast_month 
        AND b.trip_month >= DATEADD('month', -12, a.forecast_month)
    WHERE b.platform in ('Android native','iOS native')
    GROUP BY 1, 2)
UNION all
    (SELECT a.forecast_month
        , 'all google'                                                                                                        AS segment
        , SUM(b.paid_day)                                                                                                     AS paid_days
        , SUM(b.net_revenue_usd)                                                                                              AS net_revenue_usd
        , SUM(b.trip)                                                                                                           AS trips
    FROM 
        (SELECT distinct signup_month as forecast_month
        FROM #temp_signup_base
        WHERE forecast_month >= '2022-01-01') as a
    LEFT JOIN #trips_increment_1 as b 
    ON b.trip_month < a.forecast_month 
        AND b.trip_month >= DATEADD('month', -12, a.forecast_month)
    WHERE b.channels in ('Google_Desktop','Google_Mobile')
    GROUP BY 1, 2)
UNION all
    (SELECT a.forecast_month
        , 'all'                                                                                                               AS segment
        , SUM(b.paid_day)                                                                                                     AS paid_days
        , SUM(b.net_revenue_usd)                                                                                              AS net_revenue_usd
        , SUM(b.trip)                                                                                                           AS trips
    FROM 
        (SELECT distinct signup_month as forecast_month
        FROM #temp_signup_base
        WHERE forecast_month >= '2022-01-01') as a
    LEFT JOIN #trips_increment_1 as b 
    ON b.trip_month < a.forecast_month 
        AND b.trip_month >= DATEADD('month', -12, a.forecast_month)
    GROUP BY 1, 2);


------------------------------------------------------------------------------------------------------------------------
-- NRPD by channel at increment 1
------------------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS #nrpd_channel;
SELECT b.forecast_month
     , a.channels
     , b.segment
     , b.paid_days                                                                  
     , b.net_revenue_usd                                                                                                    
     , b.trips                                                                                             
INTO #nrpd_channel
FROM 
    (SELECT channels,
        -- mapping table from channel to segment
        case when channels in ('Google_Desktop', 'Google_Mobile', 'Google_Desktop_Brand', 'Google_Mobile_Brand', 'Google_Pmax',
                                'Google_UAC_Android', 'Kayak_Desktop_Carousel', 'Kayak_Desktop_Compare', 'Kayak_Desktop_Core',
                                'Kayak_Mobile_Carousel', 'Kayak_Mobile_Core', 'Microsoft_Desktop', 'Microsoft_Desktop_Brand') then channels
            when channels in ('Google_UAC_iOS') then 'all app'
            when channels in('Free_Google','Free_Microsoft','Free_Other') then 'all free'
            when channels in ('Google_Discovery','Kayak_Desktop_Front_Door', 'Kayak_Desktop', 
                               'Autorental_Desktop') then 'all desktop'
            when channels in ('Microsoft_Mobile','Autorental_Mobile','Kayak_Mobile','Hopper',
                              'Kayak_Mobile_Front_Door') then 'all mobile'
            when channels in ('Kayak_Carousel','Delta','Capital_One','Mediaalpha','Expedia', 'Facebook/IG_Web') then 'all web'
            when channels in ('Tiktok','Apple','Snapchat', 'Facebook/IG_App') then 'all app'
            else 'all paid' end as segment
    FROM 
    (SELECT distinct channels from #trips_increment_1)) as a
LEFT JOIN #nrpd_segment as b on a.segment = b.segment;

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
    -- Note that this is different from #trips_increment_1, since it includes trips from all increments
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
    , a.channels
    , a.segment
    , a.trips as data_volume
    , a.net_revenue_usd/a.paid_days as nrpd_channel
    , b.net_revenue_usd/b.paid_days as nrpd_all
from #nrpd_channel as a
left join #nrpd as b on a.forecast_month = b.forecast_month
order by 1, 2;