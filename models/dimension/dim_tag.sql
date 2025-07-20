{{ config(materialized="table", tags=["dimension"]) }}

with
    source as (
        select video_id, split(tags, ',') as tag_array from {{ ref("dim_video") }}
    ),

    unnested_tags as (select trim(tag) as tag from source, unnest(tag_array) as tag),

    distinct_tags as (
        select
            {{ dbt_utils.generate_surrogate_key(["tag"]) }} as dim_tag_key,
            tag as tag_name
        from unnested_tags
        group by tag
    )

select *
from distinct_tags