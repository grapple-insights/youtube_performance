{{ config(materialized="table", tags=["dimension"]) }} 

with
    source as (select * from {{ ref("stage_youtube_api_metrics") }}),

    video as (
        select
            -- Surrogate key for dim_video (for joining in fact models)
            {{ dbt_utils.generate_surrogate_key(["video_id"]) }} as dim_video_key,

            -- Primary business key & relationships
            video_id,
            channel_id,

            -- Core metadata
            video_name,
            video_description,
            video_url,
            video_thumbnail_url,
            video_tags,
            video_topic_categories,

            -- Dates & timestamps
            video_upload_date,
            video_published_timestamp_utc,
            extract(hour from video_published_timestamp_utc) as video_published_hour_utc,
            case
                when extract(dayofweek from video_published_timestamp_utc) in (1, 7)
                then true
                else false
            end as video_published_on_weekend,

            -- Video properties
            video_duration_seconds,
            video_definition,
            is_video_caption,
            is_licensed_content,

            -- Derived flags
            is_short,
            is_full_match,
            length(video_name) as video_name_length

        from source
    )

select *
from video