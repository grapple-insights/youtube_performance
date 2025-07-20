{{ config(materialized="table", tags=["dimension"]) }}

with
    source as (select * from {{ ref("stage_youtube_api_metrics") }}),

    ranked as (
        select
            *,
            row_number() over (
                partition by channel_id order by video_upload_date desc
            ) as row_num
        from source
    ),

    channel as (
        select
            {{ dbt_utils.generate_surrogate_key(["channel_id"]) }} as dim_channel_key,

            channel_id,
            channel_name,
            channel_subscribers

        from ranked
        where row_num = 1  -- This keeps only the latest row per channel
    )

select *
from channel