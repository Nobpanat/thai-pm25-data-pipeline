{{
  config(
    materialized='table',
    partition_by={
      "field": "measured_at",
      "data_type": "timestamp",
      "granularity": "day"
    }
  )
}}

with raw_converted as (
    select
        cast(location_id as int64) as location_id,
        cast(sensors_id as int64) as sensor_id,
        cast(location as string) as location_name,
        safe_cast(datetime as timestamp) as measured_at,
        cast(lat as float64) as latitude,
        cast(lon as float64) as longitude,
        cast(parameter as string) as parameter,
        cast(value as float64) as pm_value,
        cast(units as string) as unit 
    from {{ source('staging', 'openaq_raw_external') }}
    where value >= 0
      and location_id is not null 
      and datetime is not null
      and lat is not null
      and lon is not null
      and parameter in ('pm25', 'pm10')
)

select
    *,
    extract(year from measured_at) as year,
    extract(month from measured_at) as month
from raw_converted
where measured_at is not null