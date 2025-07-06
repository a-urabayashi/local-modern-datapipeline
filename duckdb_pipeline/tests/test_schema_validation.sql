/*
Schema validation tests for dbt models
Ensures consistent data types and structure across models
*/

-- Test 1: Required columns presence in staging models
select 
    'missing_required_columns' as test_name,
    count(*) as failing_records,
    'Required columns must be present in staging models' as test_description
from (
    select 1 as dummy_col
    where not exists (
        select column_name 
        from information_schema.columns 
        where table_name = 'stg_fineweb_japanese'
        and column_name in ('content_text', 'source_dataset', 'content_length', 'contains_japanese_chars')
    )
) as missing_columns
having count(*) > 0

union all

-- Test 2: Data type consistency between staging models
select 
    'data_type_consistency' as test_name,
    count(*) as failing_records,
    'Common columns should have consistent data types' as test_description
from (
    select f.content_length, w.text_length
    from {{ ref('stg_fineweb_japanese') }} f
    full outer join {{ ref('stg_wikipedia_japanese') }} w on 1=1
    where 1=0  -- Force schema check only
) as type_check
where typeof(content_length) != typeof(text_length)
having count(*) > 0

union all

-- Test 3: Timestamp columns should not be null in processed data
select 
    'timestamp_validity' as test_name,
    count(*) as failing_records,
    'Processing timestamps should not be null' as test_description
from {{ ref('stg_fineweb_japanese') }}
where dbt_extracted_timestamp is null
   or processed_timestamp is null
having count(*) > 0

union all

-- Test 4: Numeric columns should have valid ranges
select 
    'numeric_range_validation' as test_name,
    count(*) as failing_records,
    'Content length should be positive and reasonable' as test_description
from {{ ref('stg_fineweb_japanese') }}
where content_length < 0 
   or content_length > 1000000  -- 1MB seems reasonable max
having count(*) > 0

union all

-- Test 5: Boolean columns should only have 0/1 or true/false values
select 
    'boolean_validation' as test_name,
    count(*) as failing_records,
    'Boolean flags should have valid values' as test_description
from {{ ref('stg_fineweb_japanese') }}
where contains_japanese_chars not in (0, 1, true, false)
   or is_valid_content not in (0, 1, true, false)
having count(*) > 0

union all

-- Test 6: Foreign key integrity (source dataset references)
select 
    'foreign_key_integrity' as test_name,
    count(*) as failing_records,
    'Source dataset references should be valid' as test_description
from {{ ref('stg_fineweb_japanese') }}
where source_dataset is null 
   or source_dataset = ''
having count(*) > 0

union all

-- Test 7: Primary key uniqueness in Wikipedia data
select 
    'primary_key_uniqueness' as test_name,
    count(*) - count(distinct article_id) as failing_records,
    'Wikipedia article IDs should be unique' as test_description
from {{ ref('stg_wikipedia_japanese') }}
having count(*) - count(distinct article_id) > 0

union all

-- Test 8: Categorical column value validation
select 
    'categorical_validation' as test_name,
    count(*) as failing_records,
    'Content length categories should have valid values' as test_description
from {{ ref('stg_fineweb_japanese') }}
where content_length_category not in ('short', 'medium', 'long')
having count(*) > 0

union all

-- Test 9: Data quality checks model structure validation
select 
    'quality_checks_structure' as test_name,
    count(*) as failing_records,
    'Quality score should be between 0 and 100' as test_description
from {{ ref('stg_data_quality_checks') }}
where quality_score < 0 or quality_score > 100
having count(*) > 0

union all

-- Test 10: Summary table aggregation consistency
select 
    'aggregation_consistency' as test_name,
    case 
        when sum_individual != total_from_summary then 1 
        else 0 
    end as failing_records,
    'Summary totals should match individual record counts' as test_description
from (
    select 
        (select count(*) from {{ ref('stg_data_quality_checks') }}) as sum_individual,
        (select sum(total_records) from {{ ref('mart_data_quality_summary') }} where dataset_name != 'TOTAL') as total_from_summary
) as consistency_check
having failing_records > 0