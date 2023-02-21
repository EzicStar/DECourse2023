{{ config(materialized='view') }}

SELECT *
FROM {{ source('staging', 'fhv_2019') }}

{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}