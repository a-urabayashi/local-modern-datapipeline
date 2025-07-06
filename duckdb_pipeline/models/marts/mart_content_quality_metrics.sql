{{ config(materialized='table') }}

/*
Content Quality Metrics Mart
Analyzes quality indicators for Japanese content from both sources
*/

with fineweb_quality as (
    select
        'fineweb' as source,
        url_domain,
        ANY_VALUE(source_dataset) as processing_source,
        
        count(*) as total_items,
        avg(content_length) as avg_length,
        
        -- Quality metrics
        count(case when content_length >= 100 then 1 end) as substantial_content_count,
        count(case when content_length < 100 then 1 end) as minimal_content_count,
        
        -- Content diversity (rough estimate)
        count(distinct left(content_text, 50)) as unique_content_starts,
        
        -- Processing metrics
        min(dbt_extracted_timestamp) as first_processed,
        max(dbt_extracted_timestamp) as last_processed
        
    from {{ ref('stg_fineweb_japanese') }}
    group by url_domain, source_dataset
),

wikipedia_quality as (
    select
        'wikipedia' as source,
        article_type as url_domain,  -- Using article_type as grouping dimension
        ANY_VALUE(source_dataset) as processing_source,
        
        count(*) as total_items,
        avg(text_length) as avg_length,
        
        -- Quality metrics  
        count(case when text_length >= 500 then 1 end) as substantial_content_count,
        count(case when text_length < 500 then 1 end) as minimal_content_count,
        
        -- Content diversity
        count(distinct article_id) as unique_content_starts,
        
        -- Processing metrics
        min(dbt_extracted_timestamp) as first_processed,
        max(dbt_extracted_timestamp) as last_processed
        
    from {{ ref('stg_wikipedia_japanese') }}
    group by article_type
),

combined_quality as (
    select * from fineweb_quality
    union all
    select * from wikipedia_quality
),

quality_summary as (
    select
        source,
        url_domain as content_group,
        processing_source,
        total_items,
        round(avg_length, 2) as avg_content_length,
        substantial_content_count,
        minimal_content_count,
        unique_content_starts,
        
        -- Quality ratios
        round(100.0 * substantial_content_count / total_items, 2) as substantial_content_pct,
        round(100.0 * unique_content_starts / total_items, 2) as content_diversity_pct,
        
        -- Processing info
        first_processed,
        last_processed,
        
        -- Quality score (0-100)
        least(100, round(
            (substantial_content_pct * 0.6) + 
            (content_diversity_pct * 0.4)
        , 2)) as quality_score,
        
        current_timestamp as metrics_generated_at,
        '{{ var("batch_id", "unknown") }}' as batch_id
        
    from combined_quality
    where total_items > 0
),

final as (
    select 
        *,
        case 
            when quality_score >= 80 then 'high'
            when quality_score >= 60 then 'medium'
            when quality_score >= 40 then 'low'
            else 'poor'
        end as quality_tier
    from quality_summary
)

select * from final
order by source, quality_score desc