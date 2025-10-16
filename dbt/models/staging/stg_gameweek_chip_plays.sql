{{ config(materialized='table') }}

with gw as (
  select id, chip_plays from {{ ref('stg_gameweeks') }}
)

select
  gw.id                                  as gameweek_id,
  (cp->>'chip_name')::text               as chip_name,
  (cp->>'num_played')::int               as num_played
from gw
join lateral jsonb_array_elements(gw.chip_plays) as cp on true
where gw.chip_plays is not null