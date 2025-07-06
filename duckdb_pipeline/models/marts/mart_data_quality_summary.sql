{{ config(materialized='table') }}

with quality_data as (
    select * from {{ ref('stg_data_quality_checks') }}
),

dataset_summary as (
    select 
        _meta_dataset_name as dataset_name,
        count(*) as total_records,
        
        -- Quality distribution
        sum(case when quality_class = 'high' then 1 else 0 end) as high_quality_count,
        sum(case when quality_class = 'medium' then 1 else 0 end) as medium_quality_count,
        sum(case when quality_class = 'low' then 1 else 0 end) as low_quality_count,
        sum(case when quality_class = 'poor' then 1 else 0 end) as poor_quality_count,
        
        -- Anomaly statistics
        sum(is_anomaly) as anomaly_count,
        sum(case when text_length < 10 then 1 else 0 end) as too_short_count,
        sum(case when text_length > 50000 then 1 else 0 end) as too_long_count,
        0 as encoding_issues_count,  -- Not available in simplified model
        0 as repetition_count,       -- Not available in simplified model
        
        -- Content type distribution
        sum(case when content_type = 'content' then 1 else 0 end) as content_records,
        sum(case when content_type = 'navigation' then 1 else 0 end) as navigation_records,
        sum(case when content_type = 'ui_element' then 1 else 0 end) as ui_element_records,
        0 as date_only_records,        -- Not available in simplified model
        0 as punctuation_only_records, -- Not available in simplified model
        
        -- Text statistics
        avg(text_length) as avg_text_length,
        min(text_length) as min_text_length,
        max(text_length) as max_text_length,
        percentile_cont(0.5) within group (order by text_length) as median_text_length,
        
        -- Japanese content statistics
        avg(japanese_ratio) as avg_japanese_ratio,
        min(japanese_ratio) as min_japanese_ratio,
        max(japanese_ratio) as max_japanese_ratio,
        percentile_cont(0.5) within group (order by japanese_ratio) as median_japanese_ratio,
        
        -- Quality score statistics
        avg(quality_score) as avg_quality_score,
        min(quality_score) as min_quality_score,
        max(quality_score) as max_quality_score,
        percentile_cont(0.5) within group (order by quality_score) as median_quality_score,
        
        -- Processing metadata
        current_timestamp as earliest_processed_at,
        current_timestamp as latest_processed_at,
        1 as unique_chunks  -- Simplified for demo
        
    from quality_data
    group by _meta_dataset_name
),

overall_summary as (
    select 
        'TOTAL' as dataset_name,
        sum(total_records) as total_records,
        
        -- Quality distribution
        sum(high_quality_count) as high_quality_count,
        sum(medium_quality_count) as medium_quality_count,
        sum(low_quality_count) as low_quality_count,
        sum(poor_quality_count) as poor_quality_count,
        
        -- Anomaly statistics
        sum(anomaly_count) as anomaly_count,
        sum(too_short_count) as too_short_count,
        sum(too_long_count) as too_long_count,
        sum(encoding_issues_count) as encoding_issues_count,
        sum(repetition_count) as repetition_count,
        
        -- Content type distribution
        sum(content_records) as content_records,
        sum(navigation_records) as navigation_records,
        sum(ui_element_records) as ui_element_records,
        sum(date_only_records) as date_only_records,
        sum(punctuation_only_records) as punctuation_only_records,
        
        -- Weighted averages for text statistics
        sum(avg_text_length * total_records) / sum(total_records) as avg_text_length,
        min(min_text_length) as min_text_length,
        max(max_text_length) as max_text_length,
        null as median_text_length,  -- Cannot aggregate medians
        
        -- Weighted averages for Japanese content
        sum(avg_japanese_ratio * total_records) / sum(total_records) as avg_japanese_ratio,
        min(min_japanese_ratio) as min_japanese_ratio,
        max(max_japanese_ratio) as max_japanese_ratio,
        null as median_japanese_ratio,
        
        -- Weighted averages for quality scores
        sum(avg_quality_score * total_records) / sum(total_records) as avg_quality_score,
        min(min_quality_score) as min_quality_score,
        max(max_quality_score) as max_quality_score,
        null as median_quality_score,
        
        -- Processing metadata
        min(earliest_processed_at) as earliest_processed_at,
        max(latest_processed_at) as latest_processed_at,
        sum(unique_chunks) as unique_chunks
        
    from dataset_summary
),

final_summary as (
    select * from dataset_summary
    union all
    select * from overall_summary
)

select 
    dataset_name,
    total_records,
    
    -- Quality percentages
    round((high_quality_count * 100.0) / total_records, 2) as high_quality_pct,
    round((medium_quality_count * 100.0) / total_records, 2) as medium_quality_pct,
    round((low_quality_count * 100.0) / total_records, 2) as low_quality_pct,
    round((poor_quality_count * 100.0) / total_records, 2) as poor_quality_pct,
    
    -- Anomaly percentages
    round((anomaly_count * 100.0) / total_records, 2) as anomaly_rate_pct,
    round((too_short_count * 100.0) / total_records, 2) as too_short_pct,
    round((too_long_count * 100.0) / total_records, 2) as too_long_pct,
    round((encoding_issues_count * 100.0) / total_records, 2) as encoding_issues_pct,
    round((repetition_count * 100.0) / total_records, 2) as repetition_pct,
    
    -- Content type percentages
    round((content_records * 100.0) / total_records, 2) as content_pct,
    round((navigation_records * 100.0) / total_records, 2) as navigation_pct,
    round((ui_element_records * 100.0) / total_records, 2) as ui_element_pct,
    round((date_only_records * 100.0) / total_records, 2) as date_only_pct,
    round((punctuation_only_records * 100.0) / total_records, 2) as punctuation_only_pct,
    
    -- Text statistics
    round(avg_text_length, 0) as avg_text_length,
    min_text_length,
    max_text_length,
    median_text_length,
    
    -- Japanese content statistics
    round(avg_japanese_ratio, 3) as avg_japanese_ratio,
    round(min_japanese_ratio, 3) as min_japanese_ratio,
    round(max_japanese_ratio, 3) as max_japanese_ratio,
    round(median_japanese_ratio, 3) as median_japanese_ratio,
    
    -- Quality score statistics
    round(avg_quality_score, 1) as avg_quality_score,
    min_quality_score,
    max_quality_score,
    round(median_quality_score, 1) as median_quality_score,
    
    -- Processing metadata
    earliest_processed_at,
    latest_processed_at,
    unique_chunks,
    
    -- Data quality assessment
    case 
        when round((high_quality_count * 100.0) / total_records, 2) >= 70 then 'Excellent'
        when round((high_quality_count * 100.0) / total_records, 2) >= 50 then 'Good'
        when round((high_quality_count * 100.0) / total_records, 2) >= 30 then 'Fair'
        else 'Poor'
    end as overall_quality_assessment,
    
    -- Recommendations
    case 
        when round((anomaly_count * 100.0) / total_records, 2) > 30 then 'High anomaly rate - Review data sources'
        when round((poor_quality_count * 100.0) / total_records, 2) > 20 then 'High poor quality rate - Enhance filtering'
        when round(avg_japanese_ratio, 3) < 0.5 then 'Low Japanese content - Review language detection'
        when round((content_records * 100.0) / total_records, 2) < 80 then 'High non-content ratio - Improve content extraction'
        else 'Data quality is acceptable'
    end as quality_recommendation,
    
    current_timestamp as summary_generated_at
    
from final_summary
order by 
    case when dataset_name = 'TOTAL' then 1 else 0 end,
    dataset_name