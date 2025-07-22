SELECT
  d.calendar_date,
  dc.channel_name,
  t.tag_name,
  round(SUM(f.video_views / tag_count.num_tags), 2) AS attributed_views,
  round(SUM(f.video_likes / tag_count.num_tags), 2) AS attributed_likes,
  round(SUM(f.video_comments / tag_count.num_tags), 2) AS attributed_comments,
  ROUND(SUM((f.video_likes + f.video_comments) / tag_count.num_tags) / NULLIF(SUM(f.video_views / tag_count.num_tags), 0), 3) AS attributed_engagement_rate
FROM {{ ref("fact_video_daily_performance") }} f
JOIN {{ ref("bridge_video_tag") }} bvt ON f.dim_video_key = bvt.dim_video_key
JOIN {{ ref("dim_tag") }} t ON bvt.dim_tag_key = t.dim_tag_key
-- Add the channel dimension join:
JOIN {{ ref("dim_channel") }} dc ON f.dim_channel_key = dc.dim_channel_key
JOIN (
  SELECT dim_video_key, COUNT(*) AS num_tags
  FROM {{ ref("bridge_video_tag") }}
  GROUP BY dim_video_key
) tag_count ON f.dim_video_key = tag_count.dim_video_key
LEFT JOIN {{ ref("dim_date") }} d ON f.dim_date_key = d.dim_date_key
GROUP BY d.calendar_date, dc.channel_name, t.tag_name
ORDER BY d.calendar_date, dc.channel_name, t.tag_name 