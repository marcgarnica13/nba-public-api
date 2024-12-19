{{ config(materialized='table') }}

select *
from {{source('nba_api', 'load_info__tables__columns')}}