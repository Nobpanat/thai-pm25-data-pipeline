with raw_seed_data as (
    select * from {{ ref('thai_locations') }}
)

select
    cast(location_id as int64) as location_id,
    
    cast(original_name as string) as location_name,
    
    cast(latitude as float64) as latitude,
    cast(longitude as float64) as longitude,
    
    cast(province as string) as province,
    cast(region as string) as region
from raw_seed_data