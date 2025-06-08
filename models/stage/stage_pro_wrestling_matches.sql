-- stage table for raw pro wrestling matches
-- pulled from BigQuery
-- data from cagematch.net
select 
   SAFE.PARSE_DATE('%d.%m.%Y', `date`) AS calendar_date
 , case when `promotion` in ('All Elite Wrestling', 'New Japan Pro Wrestling') then 'AEW'
        when `promotion` = 'World Wrestling Entertainment' then 'WWE'
        end as promotion
 , match   
 , event_type_arena_location
from {{ source('pro_wrestling_events', 'raw_matches') }}
where SAFE.PARSE_DATE('%d.%m.%Y', `date`) is not null 