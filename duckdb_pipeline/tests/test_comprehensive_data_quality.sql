/*
Comprehensive dbt test suite for Japanese content data quality
Tests both data integrity and business logic validation
*/

-- Test 1: High quality data should have Japanese ratio above 50%
{{ config(severity='warn') }}
select 
    'high_quality_japanese_ratio' as test_name,
    count(*) as failing_records,
    'High quality records should have Japanese ratio >= 0.5' as test_description
from {{ ref('stg_data_quality_checks') }}
where quality_class = 'high' and japanese_ratio < 0.5
having count(*) > 0

union all

-- Test 2: Content type classification accuracy
select 
    'content_type_classification' as test_name,
    count(*) as failing_records,
    'Navigation elements should not be classified as content' as test_description
from {{ ref('stg_data_quality_checks') }}
where content_type = 'content' 
  and upper(trim(text)) in ('ホーム', 'HOME', 'トップ', 'TOP', 'メニュー', 'MENU', 'ログイン', 'LOGIN')
having count(*) > 0

union all

-- Test 3: Encoding issues detection
select 
    'encoding_issues_detection' as test_name,
    count(*) as failing_records,
    'Replacement characters should be flagged as encoding issues' as test_description
from {{ ref('stg_data_quality_checks') }}
where has_encoding_issues = 0 
  and (text like '%��%' or text like '%\uFFFD%')
having count(*) > 0

union all

-- Test 4: Quality score consistency with quality class
select 
    'quality_score_consistency' as test_name,
    count(*) as failing_records,
    'Quality scores should align with quality classifications' as test_description
from {{ ref('stg_data_quality_checks') }}
where (quality_class = 'high' and quality_score < 70)
   or (quality_class = 'medium' and (quality_score < 50 or quality_score >= 90))
   or (quality_class = 'low' and (quality_score < 30 or quality_score >= 70))
   or (quality_class = 'poor' and quality_score >= 50)
having count(*) > 0

union all

-- Test 5: Text length validation logic
select 
    'text_length_validation' as test_name,
    count(*) as failing_records,
    'Text length flags should match actual lengths' as test_description
from {{ ref('stg_data_quality_checks') }}
where (is_too_short = 1 and text_length >= 10)
   or (is_too_long = 1 and text_length <= 50000)
having count(*) > 0

union all

-- Test 6: Japanese character counting accuracy
select 
    'japanese_character_counting' as test_name,
    count(*) as failing_records,
    'Japanese character count cannot exceed total text length' as test_description
from {{ ref('stg_data_quality_checks') }}
where total_japanese_chars > text_length
having count(*) > 0

union all

-- Test 7: Ratio calculations validity
select 
    'ratio_calculations' as test_name,
    count(*) as failing_records,
    'All ratios should be between 0 and 1' as test_description
from {{ ref('stg_data_quality_checks') }}
where japanese_ratio < 0 or japanese_ratio > 1
   or ascii_ratio < 0 or ascii_ratio > 1
   or symbol_ratio < 0 or symbol_ratio > 1
   or whitespace_ratio < 0 or whitespace_ratio > 1
having count(*) > 0

union all

-- Test 8: Anomaly detection consistency
select 
    'anomaly_detection_consistency' as test_name,
    count(*) as failing_records,
    'Anomaly flag should match individual quality flags' as test_description
from {{ ref('stg_data_quality_checks') }}
where is_anomaly != case 
    when is_too_short = 1 or is_too_long = 1 or has_encoding_issues = 1 
         or content_type != 'content' or japanese_ratio < 0.3 or has_repetition = 1 
    then 1 else 0 end
having count(*) > 0

union all

-- Test 9: Summary data integrity
select 
    'summary_data_integrity' as test_name,
    count(*) as failing_records,
    'Summary percentages and totals should be valid' as test_description
from {{ ref('mart_data_quality_summary') }}
where total_records <= 0 
   or high_quality_pct + medium_quality_pct + low_quality_pct + poor_quality_pct > 100.1
   or anomaly_rate_pct > 100 or anomaly_rate_pct < 0
   or avg_japanese_ratio > 1.0 or avg_japanese_ratio < 0
   or avg_quality_score > 100 or avg_quality_score < 0
having count(*) > 0

union all

-- Test 10: Dataset completeness check
select 
    'dataset_completeness' as test_name,
    count(*) as failing_records,
    'All expected datasets should have records' as test_description
from (
    select dataset_name
    from {{ ref('mart_data_quality_summary') }}
    where dataset_name != 'TOTAL'
    and total_records = 0
) as empty_datasets
having count(*) > 0

union all

-- Test 11: Staging model data freshness
select 
    'data_freshness_fineweb' as test_name,
    count(*) as failing_records,
    'FineWeb data should be processed within last 7 days' as test_description
from {{ ref('stg_fineweb_japanese') }}
where dbt_extracted_timestamp < current_timestamp - interval '7 days'
having count(*) > 0

union all

select 
    'data_freshness_wikipedia' as test_name,
    count(*) as failing_records,
    'Wikipedia data should be processed within last 7 days' as test_description
from {{ ref('stg_wikipedia_japanese') }}
where dbt_extracted_timestamp < current_timestamp - interval '7 days'
having count(*) > 0