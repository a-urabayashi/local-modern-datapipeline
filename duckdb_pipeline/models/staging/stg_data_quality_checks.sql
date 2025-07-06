{{ config(materialized='view') }}

-- Simplified data quality checks compatible with DuckDB
with fineweb_data as (
    select 
        content_text as text,
        content_length as text_length,
        case when contains_japanese_chars then 0.8 else 0.1 end as japanese_ratio,
        case when contains_japanese_chars then 0.2 else 0.8 end as ascii_ratio,
        source_dataset as _meta_dataset_name
    from {{ ref('stg_fineweb_japanese') }}
),

wikipedia_data as (
    select 
        article_text as text,
        text_length,
        case when contains_japanese_chars then 0.9 else 0.1 end as japanese_ratio,
        case when contains_japanese_chars then 0.1 else 0.9 end as ascii_ratio,
        source_dataset as _meta_dataset_name
    from {{ ref('stg_wikipedia_japanese') }}
),

source_data as (
    select * from fineweb_data
    union all 
    select * from wikipedia_data
)

select 
    text,
    text_length,
    japanese_ratio,
    ascii_ratio,
    
    -- Quality score (0-100)
    case 
        when text_length < 10 then 0
        when text_length > 50000 then 10
        when japanese_ratio < 0.1 then 30
        when japanese_ratio < 0.3 then 50
        when japanese_ratio < 0.5 then 70
        else 90
    end as quality_score,
    
    -- Quality classification
    case 
        when text_length < 10 or text_length > 50000 then 'poor'
        when japanese_ratio < 0.3 then 'low'
        when japanese_ratio < 0.5 then 'medium'
        else 'high'
    end as quality_class,
    
    -- Content type (simplified)
    case 
        when text_length < 10 then 'ui_element'
        when japanese_ratio < 0.1 then 'navigation'
        else 'content'
    end as content_type,
    
    -- Anomaly detection (simplified)
    case 
        when text_length < 10 or text_length > 50000 or japanese_ratio < 0.3 
        then 1 
        else 0 
    end as is_anomaly,
    
    _meta_dataset_name
    
from source_data