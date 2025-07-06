{% macro setup_delta_lake_connection() %}
    -- Install and load necessary DuckDB extensions for Delta Lake and S3
    {% set extensions_sql %}
        INSTALL httpfs;
        LOAD httpfs;
        
        -- Configure S3 settings for MinIO
        SET s3_region='us-east-1';
        SET s3_url_style='path';
        SET s3_endpoint='localhost:9000';
        SET s3_use_ssl=false;
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
        
        -- Try to install delta extension if available
        -- Note: Delta Lake support in DuckDB is experimental
        {% if target.name == 'dev' %}
            -- SET enable_experimental_delta=true;
        {% endif %}
    {% endset %}
    
    {{ extensions_sql }}
{% endmacro %}

{% macro read_delta_table_with_fallback(delta_path, parquet_fallback_path) %}
    -- Macro to read Delta Lake table with parquet fallback
    {% set read_sql %}
        -- Try to read from Delta Lake first, fallback to parquet
        SELECT * FROM (
            {% if var('use_delta_lake', false) %}
                -- Delta Lake read (experimental)
                SELECT * FROM delta_scan('{{ delta_path }}')
            {% else %}
                -- Fallback to parquet files
                SELECT * FROM read_parquet('{{ parquet_fallback_path }}')
            {% endif %}
        )
    {% endset %}
    
    {{ return(read_sql) }}
{% endmacro %}

{% macro get_source_data(source_name, table_name) %}
    -- Enhanced source data retrieval with Delta Lake support
    {% if source_name == 'delta_lake_content' %}
        {{ setup_delta_lake_connection() }}
    {% endif %}
    
    {{ return(source(source_name, table_name)) }}
{% endmacro %}