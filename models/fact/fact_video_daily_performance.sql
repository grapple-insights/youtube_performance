select 
d.dim_date_key,
c.dim_channel_key,
v.dim_video_key,
v.is_short,
v.is_full_match,
stg.video_views,
stg.video_likes,
stg.video_comments,
stg.video_duration_seconds,
round((stg.video_likes + stg.video_comments) / nullif(stg.video_views, 0), 3) as video_engagement_rate
from {{ ref("stage_youtube_api_metrics") }} stg 
left join {{ ref("dim_date") }} d on stg.video_upload_date = d.calendar_date
left join {{ ref("dim_channel") }} c on stg.channel_id = c.channel_id
left join {{ ref("dim_video") }} v on stg.video_id = v.video_id 