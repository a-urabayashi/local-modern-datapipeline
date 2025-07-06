{{ config(materialized='view') }}

/*
Staging model for Wikipedia Japanese content from Delta Lake
Cleans and standardizes the Delta Lake processed Wikipedia data
*/

-- Load data directly from DuckDB wikipedia table (populated from Delta Lake)
with source_data as (
    select 
        id,
        title,
        text,
        _meta_dataset_name,
        _dbt_extracted_at,
        _dbt_source_table
    from {{ source('raw_data', 'wikipedia') }}
    where id is not null and title is not null and text is not null
),

cleaned_data as (
    select
        -- Core fields
        cast(id as varchar) as article_id,
        trim(title) as article_title,
        trim(text) as article_text,
        
        -- Metadata fields
        _meta_dataset_name as source_dataset,
        _dbt_extracted_at as dbt_extracted_timestamp,
        _dbt_source_table as dbt_source_table,
        
        -- Derived fields
        length(trim(text)) as text_length,
        length(trim(title)) as title_length,
        
        -- Article categorization
        case 
            when length(trim(text)) > 10000 then 'comprehensive'
            when length(trim(text)) > 2000 then 'detailed'
            when length(trim(text)) > 500 then 'standard'
            else 'stub'
        end as article_size_category,
        
        -- Title analysis
        case 
            when title like '%（%disambiguation%）' then 'disambiguation'
            when title like '%の一覧' then 'list'
            when title like 'カテゴリ:%' then 'category'
            else 'article'
        end as article_type,
        
        -- Japanese character analysis
        case 
            when regexp_matches(text, '[ひらがな-んカタカナ-ヶ一-龯]') then true
            else false
        end as contains_japanese_chars,
        
        -- Content quality indicators
        case 
            when text is null or trim(text) = '' then false
            when title is null or trim(title) = '' then false
            when length(trim(text)) < 50 then false
            when text like '%redirect%' or text like '%リダイレクト%' then false
            else true
        end as is_valid_article,
        
        -- Extract first paragraph for summary
        split_part(text, '\n\n', 1) as first_paragraph,
        
        -- Count sections (rough estimate)
        (length(text) - length(replace(text, '\n==', ''))) as section_count_estimate

    from source_data
    where id is not null
    and title is not null
),

final as (
    select 
        *,
        -- Processing metadata
        current_timestamp as dbt_updated_at,
        '{{ var("batch_id", "unknown") }}' as dbt_batch_id
    from cleaned_data
    where is_valid_article = true
    and contains_japanese_chars = true
    and article_type = 'article'  -- Focus on actual articles
)

select * from final