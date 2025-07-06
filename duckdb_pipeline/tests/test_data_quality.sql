-- Test: High quality data should have Japanese ratio above 30%
select count(*) as failing_records
from {{ ref('stg_data_quality_checks') }}
where quality_class = 'high' and japanese_ratio < 0.3

-- Test: Content type should be classified correctly
-- This test ensures navigation elements are properly identified
select count(*) as failing_records
from {{ ref('stg_data_quality_checks') }}
where content_type = 'content' 
  and (text in ('ホーム', 'HOME', 'トップ', 'TOP', 'メニュー', 'MENU'))

-- Test: Anomaly detection should catch encoding issues
select count(*) as failing_records
from {{ ref('stg_data_quality_checks') }}
where has_encoding_issues = 0 
  and text like '%��%'

-- Test: Quality score should be consistent with quality class
select count(*) as failing_records
from {{ ref('stg_data_quality_checks') }}
where (quality_class = 'high' and quality_score < 70)
   or (quality_class = 'medium' and (quality_score < 50 or quality_score >= 80))
   or (quality_class = 'low' and (quality_score < 30 or quality_score >= 70))
   or (quality_class = 'poor' and quality_score >= 50)

-- Test: Text length validation
select count(*) as failing_records
from {{ ref('stg_data_quality_checks') }}
where (is_too_short = 1 and text_length >= 10)
   or (is_too_long = 1 and text_length <= 50000)

-- Test: Japanese character counting accuracy
select count(*) as failing_records
from {{ ref('stg_data_quality_checks') }}
where total_japanese_chars > text_length  -- Impossible condition

-- Test: Summary data integrity
select count(*) as failing_records
from {{ ref('mart_data_quality_summary') }}
where total_records <= 0 
   or high_quality_pct + medium_quality_pct + low_quality_pct + poor_quality_pct > 100.1
   or anomaly_rate_pct > 100
   or avg_japanese_ratio > 1.0
   or avg_quality_score > 100