-- stage table for raw pro wrestling events
-- pulled from BigQuery
-- data from cagematch.net
select 
  SAFE.PARSE_DATE('%d.%m.%Y', `date`) AS calendar_date
, event_name
, location
, arena
, SAFE_CAST(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          LOWER(attendance), 
          r'^(ca\.|approx\.|~|about|around)\s*', ''   -- Remove common prefixes
        ),
        r'[^0-9]', ''                                 -- Remove all non-numeric characters
      ),
      r'^0+', ''                                      -- Remove leading zeros
    ) AS INT64
  ) AS attendance
, event_type
, broadcast_type
, broadcast_network 
from {{ source('pro_wrestling_events', 'raw_events') }}