{{ config(
        materialized='incremental',
        unique_key='tripid'
    )
}}

with trips_unioned as (
    select 
        *, 
        'FHV' as service_type
    from {{ ref('stg_fhv') }}
    {% if is_incremental() %}
        WHERE pickup_datetime >= (SELECT date_trunc('month', MAX(pickup_datetime)) from {{ this }})
    {% endif %}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select trips_unioned.tripid, 
    trips_unioned.service_type,
    trips_unioned.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    trips_unioned.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    trips_unioned.pickup_datetime, 
    trips_unioned.dropoff_datetime, 
    trips_unioned.trip_type
from trips_unioned
inner join dim_zones as pickup_zone
    on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on trips_unioned.dropoff_locationid = dropoff_zone.locationid