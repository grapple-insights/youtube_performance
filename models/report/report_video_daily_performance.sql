select  
d.calendar_date,
d.calendar_day_name,
d.calendar_month_name,
d.calendar_quarter,
c.channel_id,
c.channel_name,
v.video_id,
v.video_name,
v.video_description,
v.video_url,
v.video_thumbnail_url,
v.video_published_timestamp_utc,
v.video_published_hour_utc,
f.video_duration_seconds,
f.video_views,
f.video_likes,
f.video_comments,
f.video_engagement_rate,
f.is_short,
f.is_full_match
from {{ ref("fact_video_daily_performance") }} f
left join {{ ref("dim_date") }} d on f.dim_date_key = d.dim_date_key
left join {{ ref("dim_channel") }} c on f.dim_channel_key = c.dim_channel_key
left join {{ ref("dim_video") }} v on f.dim_video_key = v.dim_video_key