{{ config(materialized="table", tags=["bridge"]) }}

with
    source as (
        select dim_video_key, split(video_tags, ',') as tag_array from {{ ref("dim_video") }}
    ),

    exploded as (
        select dim_video_key, trim(tag) as tag from source, unnest(tag_array) as tag
    ),

    joined as (
        select
            {{ dbt_utils.generate_surrogate_key(["tag"]) }} as dim_tag_key,
            dim_video_key
        from exploded
    )

select *
from joined 