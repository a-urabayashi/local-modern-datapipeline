{{ config(materialized='view') }}

/*
Staging model for FineWeb Japanese content from Delta Lake
Cleans and standardizes the Delta Lake processed FineWeb data
*/

-- Load data directly from DuckDB fineweb table (populated from Delta Lake)
with source_data as (
    select 
        text,
        url,
        timestamp,
        _meta_dataset_name,
        _dbt_extracted_at,
        _dbt_source_table
    from {{ source('raw_data', 'fineweb') }}
    where text is not null
),

cleaned_data as (
    select
        -- Content fields
        trim(text) as content_text,
        coalesce(url, 'unknown') as source_url,
        timestamp as content_timestamp,
        
        -- Metadata fields  
        _meta_dataset_name as source_dataset,
        _dbt_extracted_at as dbt_extracted_timestamp,
        _dbt_source_table as dbt_source_table,
        
        -- Derived fields
        length(trim(text)) as content_length,
        case 
            when length(trim(text)) > 1000 then 'long'
            when length(trim(text)) > 100 then 'medium'
            else 'short'
        end as content_length_category,
        
        -- Japanese text detection (basic heuristic)
        case 
            when regexp_matches(text, '[ひらがな-んカタカナ-ヶ一-龯]') then true
            else false
        end as contains_japanese_chars,
        
        -- URL domain extraction
        case 
            when url like 'http%' then 
                regexp_extract(url, 'https?://([^/]+)', 1)
            else null
        end as url_domain,
        
        -- Content quality indicators
        case 
            when text is null or trim(text) = '' then false
            when length(trim(text)) < 10 then false
            else true
        end as is_valid_content,
        
        -- Row uniqueness check
        row_number() over (
            partition by text, url 
            order by _dbt_extracted_at desc
        ) as row_rank

    from source_data
    where text is not null
),

final as (
    select 
        *,
        -- Processing metadata
        current_timestamp as dbt_updated_at,
        '{{ var("batch_id", "unknown") }}' as dbt_batch_id
    from cleaned_data
    where is_valid_content = true
    and contains_japanese_chars = true
    and row_rank = 1  -- Deduplicate
)

select * from final