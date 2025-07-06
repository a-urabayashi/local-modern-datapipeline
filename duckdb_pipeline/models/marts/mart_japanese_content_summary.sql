{{ config(materialized='table') }}

/*
Japanese Content Summary Mart with Delta Lake Processing Insights
Provides aggregated insights across FineWeb and Wikipedia Japanese content
Including anomaly detection results from Delta Lake processing pipeline
*/

with fineweb_summary as (
    select
        'fineweb' as source_type,
        count(*) as total_records,
        sum(content_length) as total_characters,
        avg(content_length) as avg_content_length,
        count(distinct url_domain) as unique_domains,
        count(distinct source_dataset) as processing_sources,
        min(dbt_extracted_timestamp) as earliest_extraction,
        max(dbt_extracted_timestamp) as latest_extraction,
        
        -- Content length distribution
        count(case when content_length_category = 'short' then 1 end) as short_content_count,
        count(case when content_length_category = 'medium' then 1 end) as medium_content_count,
        count(case when content_length_category = 'long' then 1 end) as long_content_count
        
    from {{ ref('stg_fineweb_japanese') }}
),

wikipedia_summary as (
    select
        'wikipedia' as source_type,
        count(*) as total_records,
        sum(text_length) as total_characters,
        avg(text_length) as avg_content_length,
        count(distinct article_id) as unique_domains,  -- Use consistent column name
        1 as processing_chunks,  -- Wikipedia is processed as one chunk
        min(dbt_extracted_timestamp) as earliest_extraction,
        max(dbt_extracted_timestamp) as latest_extraction,
        
        -- Article size distribution
        count(case when article_size_category = 'stub' then 1 end) as short_content_count,
        count(case when article_size_category = 'standard' then 1 end) as medium_content_count,
        count(case when article_size_category = 'detailed' then 1 end) + 
        count(case when article_size_category = 'comprehensive' then 1 end) as long_content_count
        
    from {{ ref('stg_wikipedia_japanese') }}
),

combined_summary as (
    select * from fineweb_summary
    union all
    select * from wikipedia_summary
),

final_summary as (
    select
        source_type,
        total_records,
        total_characters,
        round(avg_content_length, 2) as avg_content_length,
        unique_domains as unique_items,
        processing_sources,
        earliest_extraction,
        latest_extraction,
        short_content_count,
        medium_content_count, 
        long_content_count,
        
        -- Percentages
        round(100.0 * short_content_count / total_records, 2) as short_content_pct,
        round(100.0 * medium_content_count / total_records, 2) as medium_content_pct,
        round(100.0 * long_content_count / total_records, 2) as long_content_pct,
        
        -- Processing metadata
        current_timestamp as summary_generated_at,
        '{{ var("batch_id", "unknown") }}' as batch_id
        
    from combined_summary
)

select * from final_summary