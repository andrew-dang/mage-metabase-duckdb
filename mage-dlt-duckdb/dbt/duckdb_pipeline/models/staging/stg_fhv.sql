{{
    config(
        materialized='view'
    )
}}

WITH tripdata AS 
(
  select 
    dispatching_base_num,
    pickup_datetime, 
    dropoff_datetime AS dropoff_datetime, 
    pulocationid AS pulocationid, 
    dolocationid AS dolocationid,
    sr_flag,
    affiliated_base_number 
  from 
    {{ source('staging','fhv_pq_test') }}
  where 
    dispatching_base_num IS NOT NULL
    AND EXTRACT(YEAR FROM pickup_datetime) = 2019
)

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    sr_flag,
    -- fhv are always dispatched according to dictionary
    2 AS trip_type
from tripdata


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) == 'true' %}

  limit 100

{% endif %}
