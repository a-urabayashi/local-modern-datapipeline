{{ config(materialized='table') }}

/*
Delta Lake Processing Summary Mart
Comprehensive summary of Delta Lake processing pipeline results
Including anomaly detection, data quality metrics, and processing performance
*/

with processing_metadata as (
    select
        -- Processing run information
        '{{ var("batch_id") }}' as batch_id,
        current_timestamp as report_generated_at,
        '{{ var("use_delta_lake", true) }}' as used_delta_lake,
        
        -- Anomaly detection results from FastAPI pipeline
        {% set anomaly_results = var('anomaly_detection_results', {}) %}
        {% if anomaly_results %}
            -- FineWeb anomaly data
            {{ anomaly_results.get('fineweb', {}).get('total_records', 0) }} as fineweb_total_records,
            {{ anomaly_results.get('fineweb', {}).get('japanese_ratio', 0.0) }} as fineweb_japanese_ratio,
            {{ anomaly_results.get('fineweb', {}).get('anomaly_rate', 0.0) }} as fineweb_anomaly_rate,
            '{{ anomaly_results.get('fineweb', {}).get('analysis_method', 'unknown') }}' as fineweb_analysis_method,
            {{ anomaly_results.get('fineweb', {}).get('mock_mode', true) }} as fineweb_mock_mode,
            
            -- Wikipedia anomaly data  
            {{ anomaly_results.get('wikipedia', {}).get('total_records', 0) }} as wikipedia_total_records,
            {{ anomaly_results.get('wikipedia', {}).get('japanese_ratio', 0.0) }} as wikipedia_japanese_ratio,
            {{ anomaly_results.get('wikipedia', {}).get('anomaly_rate', 0.0) }} as wikipedia_anomaly_rate,
            '{{ anomaly_results.get('wikipedia', {}).get('analysis_method', 'unknown') }}' as wikipedia_analysis_method,
            {{ anomaly_results.get('wikipedia', {}).get('mock_mode', true) }} as wikipedia_mock_mode
        {% else %}
            -- Default values when no anomaly results available
            0 as fineweb_total_records,
            0.0 as fineweb_japanese_ratio,
            0.0 as fineweb_anomaly_rate,
            'not_available' as fineweb_analysis_method,
            true as fineweb_mock_mode,
            
            0 as wikipedia_total_records,
            0.0 as wikipedia_japanese_ratio,
            0.0 as wikipedia_anomaly_rate,
            'not_available' as wikipedia_analysis_method,
            true as wikipedia_mock_mode
        {% endif %}
),

dbt_processing_summary as (
    select
        -- dbt model processing results
        count(*) as total_fineweb_records_processed,
        avg(content_length) as avg_fineweb_content_length,
        sum(case when contains_japanese_chars then 1 else 0 end) as fineweb_japanese_detected_records,
        count(distinct url_domain) as fineweb_unique_domains
    from {{ ref('stg_fineweb_japanese') }}
    
    union all
    
    select
        count(*) as total_wikipedia_records_processed,
        avg(text_length) as avg_wikipedia_content_length,
        sum(case when contains_japanese_chars then 1 else 0 end) as wikipedia_japanese_detected_records,
        count(distinct article_id) as wikipedia_unique_articles
    from {{ ref('stg_wikipedia_japanese') }}
),

quality_comparison as (
    select
        pm.*,
        
        -- Calculate quality scores and comparisons
        round((pm.fineweb_japanese_ratio * 100), 2) as fineweb_japanese_percentage,
        round((pm.fineweb_anomaly_rate * 100), 2) as fineweb_anomaly_percentage,
        round((pm.wikipedia_japanese_ratio * 100), 2) as wikipedia_japanese_percentage,
        round((pm.wikipedia_anomaly_rate * 100), 2) as wikipedia_anomaly_percentage,
        
        -- Overall pipeline quality score
        round(
            (pm.fineweb_japanese_ratio * 0.4) + 
            (pm.wikipedia_japanese_ratio * 0.4) +
            ((1 - pm.fineweb_anomaly_rate) * 0.1) +
            ((1 - pm.wikipedia_anomaly_rate) * 0.1), 
            3
        ) as overall_quality_score,
        
        -- Processing method assessment
        case 
            when pm.fineweb_mock_mode = false and pm.wikipedia_mock_mode = false then 'full_real_analysis'
            when pm.fineweb_mock_mode = false or pm.wikipedia_mock_mode = false then 'partial_real_analysis'
            else 'mock_analysis_only'
        end as analysis_reliability,
        
        -- Data freshness
        case 
            when pm.used_delta_lake = 'true' then 'delta_lake_processed'
            else 'parquet_fallback'
        end as data_source_type
        
    from processing_metadata pm
),

final_summary as (
    select
        *,
        
        -- Summary insights
        case 
            when overall_quality_score >= 0.9 then 'excellent'
            when overall_quality_score >= 0.8 then 'good'
            when overall_quality_score >= 0.7 then 'acceptable'
            else 'needs_improvement'
        end as quality_assessment,
        
        -- Recommendations
        case
            when fineweb_anomaly_percentage > 20 then 'review_fineweb_filtering'
            when wikipedia_anomaly_percentage > 10 then 'review_wikipedia_filtering'
            when analysis_reliability = 'mock_analysis_only' then 'implement_real_analysis'
            else 'monitoring_recommended'
        end as processing_recommendation,
        
        -- Total records across both datasets
        (fineweb_total_records + wikipedia_total_records) as total_records_analyzed,
        
        -- Average quality metrics
        round((fineweb_japanese_ratio + wikipedia_japanese_ratio) / 2, 3) as avg_japanese_ratio,
        round((fineweb_anomaly_rate + wikipedia_anomaly_rate) / 2, 3) as avg_anomaly_rate
        
    from quality_comparison
)

select * from final_summary