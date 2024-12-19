{{ config(materialized='table') }}

select
    person_id,
    display_last_comma_first
from {{source('nba_data', 'players')}} as players
where team_name = 'Rockets'