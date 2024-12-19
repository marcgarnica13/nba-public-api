{{ config(materialized='table') }}

SELECT distinct team_id, team_city, team_name, team_abbreviation, team_slug, team_code,
FROM {{ source('nba_data', 'players') }}