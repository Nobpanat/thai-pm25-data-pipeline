{{
  config(
    partition_by={
      "field": "date_day",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["region", "province", "location_id"],
    description="Daily aggregated air quality data for Thailand joined by coordinates to handle ID re-indexing."
  )
}}

with air_stg as (
    select * from {{ ref('stg_pm25') }}
),

dim_locations as (
    select * from {{ ref('dim_thai_locations') }}
),

daily_agg as (
    select
        location_id,
        latitude,
        longitude,
        date(measured_at) as date_day,
        
        -- Metrics Aggregation
        round(avg(case when parameter = 'pm25' then pm_value end), 2) as avg_pm25,
        round(avg(case when parameter = 'pm10' then pm_value end), 2) as avg_pm10,
        
        max(case when parameter = 'pm25' then pm_value end) as max_pm25,
        max(case when parameter = 'pm10' then pm_value end) as max_pm10,
        
        min(case when parameter = 'pm25' then pm_value end) as min_pm25,
        min(case when parameter = 'pm10' then pm_value end) as min_pm10,
        
        max(unit) as measurement_unit,
        count(distinct sensor_id) as active_sensors,
        count(*) as total_records_count 
    from air_stg
    group by 1, 2, 3, 4
)

select
    {{ dbt_utils.generate_surrogate_key([
        'coalesce(cast(l.location_id as string), cast(a.location_id as string))', 
        'a.date_day'
    ]) }} as pm_daily_id,
    
    a.date_day,
    coalesce(l.location_id, a.location_id) as location_id,
    
    coalesce(l.location_name, 'New Station: ' || cast(a.location_id as string)) as location_name,
    coalesce(l.province, 'Unknown') as province,
    coalesce(l.region, 'Other') as region,
    
    coalesce(l.latitude, a.latitude) as latitude,
    coalesce(l.longitude, a.longitude) as longitude,
    
    concat(
        cast(coalesce(l.latitude, a.latitude) as string), ',', 
        cast(coalesce(l.longitude, a.longitude) as string)
    ) as station_location_str,
    
    st_geogpoint(coalesce(l.longitude, a.longitude), coalesce(l.latitude, a.latitude)) as location_geom,
    
    -- Metrics
    a.avg_pm25,
    a.max_pm25,
    a.min_pm25,
    a.avg_pm10,
    a.max_pm10,
    a.min_pm10,
    
    -- Info
    a.measurement_unit,
    a.active_sensors,
    a.total_records_count,
    
    case 
        when a.avg_pm25 is null then 'No PM2.5 Data'
        when a.avg_pm25 <= 15 then 'Good'
        when a.avg_pm25 <= 25 then 'Satisfactory'
        when a.avg_pm25 <= 37.5 then 'Moderate'
        when a.avg_pm25 <= 75 then 'Unhealthy'
        else 'Very Unhealthy'
    end as air_quality_status

from daily_agg a
left join dim_locations l 
    on round(a.latitude, 4) = round(l.latitude, 4)
   and round(a.longitude, 4) = round(l.longitude, 4)