"""
HuggingFace dataset processing utilities for improved parallel processing.
"""

import logging
import os
import time
import gc
from typing import Iterator, Dict, Any
from io import BytesIO
from contextlib import contextmanager

import pandas as pd
import boto3
from datasets import load_dataset, IterableDataset
from botocore.exceptions import ClientError
import psutil

# Delta Lake imports
try:
    from pyspark.sql import SparkSession
    from delta import DeltaTable, configure_spark_with_delta_pip
    DELTA_AVAILABLE = True
except ImportError:
    DELTA_AVAILABLE = False
    SparkSession = None
    DeltaTable = None


logger = logging.getLogger(__name__)


class HuggingFaceProcessor:
    """Utility class for processing HuggingFace datasets with optimizations."""
    
    def __init__(self, 
                 s3_endpoint: str = None,
                 s3_access_key: str = None, 
                 s3_secret_key: str = None,
                 bucket_name: str = None):
        """Initialize processor with S3 configuration from environment variables."""
        self.s3_endpoint = s3_endpoint or os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
        self.s3_access_key = s3_access_key or os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
        self.s3_secret_key = s3_secret_key or os.getenv('MINIO_SECRET_KEY', 'minioadmin')
        self.bucket_name = bucket_name or os.getenv('MINIO_BUCKET_NAME', 'datalake')
        self._s3_client = None
        self._spark = None
        
        # Log configuration (excluding sensitive values)
        logger.info(f"HuggingFaceProcessor initialized: endpoint={self.s3_endpoint}, bucket={self.bucket_name}")
    
    @property
    def spark(self):
        """Lazy initialization of Spark session with Delta Lake configuration."""
        if self._spark is None and DELTA_AVAILABLE:
            try:
                # Configure Spark for Delta Lake and S3
                builder = SparkSession.builder \
                    .appName("HuggingFaceProcessor") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .config("spark.hadoop.fs.s3a.endpoint", self.s3_endpoint) \
                    .config("spark.hadoop.fs.s3a.access.key", self.s3_access_key) \
                    .config("spark.hadoop.fs.s3a.secret.key", self.s3_secret_key) \
                    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
                    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                
                self._spark = configure_spark_with_delta_pip(builder).getOrCreate()
                self._spark.sparkContext.setLogLevel("WARN")
                logger.info("Spark session with Delta Lake initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Spark session: {e}")
                raise
        return self._spark
    
    @property
    def s3_client(self):
        """Lazy initialization of S3 client."""
        if self._s3_client is None:
            try:
                self._s3_client = boto3.client(
                    "s3", 
                    endpoint_url=self.s3_endpoint,
                    aws_access_key_id=self.s3_access_key,
                    aws_secret_access_key=self.s3_secret_key,
                    region_name=os.getenv('MINIO_REGION', 'us-east-1')
                )
                # Test connection
                self._s3_client.head_bucket(Bucket=self.bucket_name)
                logger.info(f"S3 client initialized successfully for bucket: {self.bucket_name}")
            except Exception as e:
                logger.error(f"Failed to initialize S3 client: {e}")
                raise
        return self._s3_client
    
    @contextmanager
    def dataset_stream(self, dataset_name: str, config_name: str = None, split: str = "train"):
        """Context manager for dataset streaming with timeout settings."""
        import os
        
        # Set HuggingFace timeout settings
        original_timeout = os.environ.get('HF_HUB_HTTP_TIMEOUT')
        original_retries = os.environ.get('HF_HUB_HTTP_RETRIES')
        
        try:
            # Increase timeout and retries for better reliability
            os.environ['HF_HUB_HTTP_TIMEOUT'] = '120'  # 2 minutes
            os.environ['HF_HUB_HTTP_RETRIES'] = '3'    # 3 retries
            
            dataset = load_dataset(
                dataset_name, 
                name=config_name, 
                split=split, 
                streaming=True,
                trust_remote_code=True
            )
            yield dataset
        except Exception as e:
            logger.error(f"Failed to load dataset {dataset_name}: {e}")
            raise
        finally:
            # Restore original settings
            if original_timeout:
                os.environ['HF_HUB_HTTP_TIMEOUT'] = original_timeout
            else:
                os.environ.pop('HF_HUB_HTTP_TIMEOUT', None)
                
            if original_retries:
                os.environ['HF_HUB_HTTP_RETRIES'] = original_retries
            else:
                os.environ.pop('HF_HUB_HTTP_RETRIES', None)
    
    def stream_chunk_batches(self, 
                           dataset: IterableDataset, 
                           start_idx: int, 
                           chunk_size: int,
                           batch_size: int = 100,
                           gc_frequency: int = 10) -> Iterator[pd.DataFrame]:
        """Stream data in smaller batches to reduce memory usage."""
        
        # Skip to start position
        dataset_iter = iter(dataset)
        for _ in range(start_idx):
            try:
                next(dataset_iter)
            except StopIteration:
                logger.warning(f"Dataset exhausted before reaching start index {start_idx}")
                return
        
        # Process in batches with memory monitoring
        processed_count = 0
        batch_count = 0
        
        while processed_count < chunk_size:
            batch_data = []
            current_batch_size = min(batch_size, chunk_size - processed_count)
            
            # Monitor memory before processing
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 85:  # If memory usage > 85%
                logger.warning(f"High memory usage: {memory_percent:.1f}% - triggering garbage collection")
                gc.collect()
                time.sleep(1)  # Brief pause to allow memory cleanup
            
            for _ in range(current_batch_size):
                try:
                    item = next(dataset_iter)
                    # Only keep essential fields to reduce memory
                    if isinstance(item, dict):
                        essential_item = {k: v for k, v in item.items() if k in ['text', 'url', 'id', 'title']}
                        batch_data.append(essential_item)
                    else:
                        batch_data.append(item)
                except StopIteration:
                    logger.info(f"Dataset exhausted after {processed_count + len(batch_data)} records")
                    break
            
            if not batch_data:
                break
            
            # Create DataFrame and yield immediately to free memory
            df = pd.DataFrame(batch_data)
            batch_size_actual = len(batch_data)
            yield df
            
            # Clear batch data immediately
            batch_data.clear()
            del df
            
            processed_count += batch_size_actual
            batch_count += 1
            
            # Periodic garbage collection
            if batch_count % gc_frequency == 0:
                gc.collect()
                memory_usage = psutil.virtual_memory().percent
                logger.info(f"Memory usage after GC: {memory_usage:.1f}%")
            
            logger.debug(f"Processed batch {batch_count}: {batch_size_actual} records, total: {processed_count}")
    
    def upload_dataframe_to_s3(self, 
                             df: pd.DataFrame, 
                             key: str,
                             max_retries: int = 3) -> bool:
        """Upload DataFrame as Parquet to S3 with retry logic."""
        
        for attempt in range(max_retries):
            try:
                buffer = BytesIO()
                # Use more efficient parquet options for memory
                df.to_parquet(
                    buffer, 
                    index=False, 
                    engine='pyarrow', 
                    compression='snappy',
                    row_group_size=1000  # Smaller row groups for better memory usage
                )
                buffer.seek(0)
                
                self.s3_client.upload_fileobj(
                    buffer, 
                    Bucket=self.bucket_name, 
                    Key=key,
                    ExtraArgs={
                        'ContentType': 'application/octet-stream',
                        'Metadata': {
                            'rows': str(len(df)),
                            'columns': str(len(df.columns)),
                            'upload_time': str(int(time.time()))
                        }
                    }
                )
                
                logger.info(f"Successfully uploaded {len(df)} rows to s3://{self.bucket_name}/{key}")
                return True
                
            except ClientError as e:
                logger.warning(f"S3 upload attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to upload {key} after {max_retries} attempts")
                    raise
                time.sleep(2 ** attempt)  # Exponential backoff
        
        return False
    
    def check_s3_object_exists(self, key: str) -> bool:
        """Check if S3 object already exists."""
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError:
            return False
    
    def write_dataframe_to_delta(self, 
                               df: pd.DataFrame, 
                               table_path: str,
                               mode: str = "append",
                               partition_cols: list = None) -> bool:
        """Write DataFrame to Delta Lake table on S3."""
        if not DELTA_AVAILABLE:
            logger.error("Delta Lake is not available. Install delta-spark package.")
            return False
            
        try:
            # Convert pandas DataFrame to Spark DataFrame
            spark_df = self.spark.createDataFrame(df)
            
            # Add processing metadata columns
            from pyspark.sql.functions import current_timestamp, lit
            spark_df = spark_df.withColumn("_processing_timestamp", current_timestamp())
            
            # Write to Delta Lake
            writer = spark_df.write.format("delta").mode(mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            # Write to S3 path
            s3_path = f"s3a://{self.bucket_name}/{table_path}"
            writer.save(s3_path)
            
            logger.info(f"Successfully wrote {len(df)} rows to Delta table: {s3_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to write DataFrame to Delta table {table_path}: {e}")
            return False
    
    def append_to_delta_table(self,
                            df: pd.DataFrame,
                            table_path: str,
                            merge_keys: list = None) -> bool:
        """Append or merge data to existing Delta table."""
        if not DELTA_AVAILABLE:
            logger.error("Delta Lake is not available. Install delta-spark package.")
            return False
            
        try:
            s3_path = f"s3a://{self.bucket_name}/{table_path}"
            
            # Check if table exists
            try:
                delta_table = DeltaTable.forPath(self.spark, s3_path)
                table_exists = True
            except Exception:
                table_exists = False
            
            if not table_exists:
                # Create new table
                return self.write_dataframe_to_delta(df, table_path, mode="overwrite")
            
            # Convert pandas DataFrame to Spark DataFrame
            spark_df = self.spark.createDataFrame(df)
            
            if merge_keys:
                # Perform merge/upsert operation
                merge_condition = " AND ".join([f"existing.{key} = updates.{key}" for key in merge_keys])
                
                delta_table.alias("existing") \
                    .merge(spark_df.alias("updates"), merge_condition) \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
                
                logger.info(f"Successfully merged {len(df)} rows to Delta table: {s3_path}")
            else:
                # Simple append
                spark_df.write.format("delta").mode("append").save(s3_path)
                logger.info(f"Successfully appended {len(df)} rows to Delta table: {s3_path}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to append to Delta table {table_path}: {e}")
            return False
    
    def read_delta_table(self, table_path: str) -> pd.DataFrame:
        """Read data from Delta Lake table and return as pandas DataFrame."""
        if not DELTA_AVAILABLE:
            logger.error("Delta Lake is not available. Install delta-spark package.")
            return pd.DataFrame()
            
        try:
            s3_path = f"s3a://{self.bucket_name}/{table_path}"
            
            # Read Delta table using Spark
            spark_df = self.spark.read.format("delta").load(s3_path)
            
            # Convert to pandas DataFrame (be careful with large datasets)
            pandas_df = spark_df.toPandas()
            
            logger.info(f"Successfully read {len(pandas_df)} rows from Delta table: {s3_path}")
            return pandas_df
            
        except Exception as e:
            logger.error(f"Failed to read Delta table {table_path}: {e}")
            return pd.DataFrame()
    
    def list_delta_tables(self, prefix: str = "delta") -> list:
        """List all Delta tables under the given prefix."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix,
                Delimiter='/'
            )
            
            tables = []
            if 'CommonPrefixes' in response:
                for obj in response['CommonPrefixes']:
                    table_path = obj['Prefix'].rstrip('/')
                    tables.append(table_path)
            
            logger.info(f"Found {len(tables)} Delta tables under prefix: {prefix}")
            return tables
            
        except Exception as e:
            logger.error(f"Failed to list Delta tables: {e}")
            return []
    
    def process_chunk_with_streaming_upload(self,
                                          dataset_name: str,
                                          config_name: str,
                                          start_idx: int,
                                          end_idx: int,
                                          batch_size: int,
                                          s3_key_prefix: str,
                                          chunk_index: int) -> Dict[str, Any]:
        """Process a chunk of data and write to Delta Lake table."""
        chunk_size = end_idx - start_idx
        total_records = 0
        
        # Generate Delta table path
        dataset_short = dataset_name.split('/')[-1]
        config_part = f"_{config_name}" if config_name else ""
        table_path = f"delta/{s3_key_prefix}/{dataset_short}{config_part}"
        
        try:
            with self.dataset_stream(dataset_name, config_name) as dataset:
                # Collect all batches for this chunk
                all_chunk_data = []
                
                for batch_df in self.stream_chunk_batches(
                    dataset, start_idx, chunk_size, batch_size
                ):
                    if validate_chunk_data(batch_df):
                        all_chunk_data.append(batch_df)
                        total_records += len(batch_df)
                        logger.info(f"Collected batch: {len(batch_df)} records, total: {total_records}")
                    
                    # Memory check
                    if psutil.virtual_memory().percent > 90:
                        logger.warning("High memory usage, breaking early")
                        break
                
                if not all_chunk_data:
                    return {
                        "status": "no_data",
                        "delta_table_path": table_path,
                        "records": 0,
                        "message": "No valid data found"
                    }
                
                # Combine all batches into single DataFrame
                combined_df = pd.concat(all_chunk_data, ignore_index=True)
                
                # Add metadata columns
                combined_df['_meta_dataset_name'] = dataset_name
                combined_df['_meta_config_name'] = config_name
                combined_df['_meta_chunk_index'] = chunk_index
                combined_df['_meta_start_idx'] = start_idx
                combined_df['_meta_end_idx'] = end_idx
                
                # Write to Delta Lake table
                if DELTA_AVAILABLE:
                    # Use merge/upsert to avoid duplicates
                    merge_keys = ['_meta_chunk_index', '_meta_start_idx', '_meta_end_idx']
                    write_success = self.append_to_delta_table(
                        combined_df, 
                        table_path, 
                        merge_keys=merge_keys
                    )
                else:
                    # Fallback to Parquet if Delta is not available
                    s3_key = generate_s3_key(
                        dataset_name, config_name, chunk_index, 
                        start_idx, end_idx, s3_key_prefix
                    )
                    write_success = self.upload_dataframe_to_s3(combined_df, s3_key)
                    table_path = s3_key
                
                if write_success:
                    return {
                        "status": "success",
                        "delta_table_path": table_path,
                        "records": total_records,
                        "file_size_mb": combined_df.memory_usage(deep=True).sum() / 1024 / 1024,
                        "format": "delta" if DELTA_AVAILABLE else "parquet"
                    }
                else:
                    return {
                        "status": "write_failed",
                        "delta_table_path": table_path,
                        "records": total_records,
                        "message": "Failed to write to Delta Lake"
                    }
                    
        except Exception as e:
            logger.error(f"Error processing chunk {chunk_index}: {e}")
            return {
                "status": "error",
                "delta_table_path": table_path if 'table_path' in locals() else "unknown",
                "records": total_records,
                "error": str(e)
            }


def create_dataset_metadata(dataset_name: str, 
                          config_name: str,
                          chunk_index: int,
                          start_idx: int, 
                          end_idx: int,
                          actual_records: int) -> Dict[str, Any]:
    """Create metadata for processed chunk."""
    return {
        'dataset_name': dataset_name,
        'config_name': config_name,
        'chunk_index': chunk_index,
        'start_idx': start_idx,
        'end_idx': end_idx,
        'actual_records': actual_records,
        'processed_at': pd.Timestamp.now().isoformat(),
        'processor_version': '1.0.0'
    }


def generate_s3_key(dataset_name: str, 
                   config_name: str,
                   chunk_index: int,
                   start_idx: int,
                   end_idx: int,
                   prefix: str = "raw") -> str:
    """Generate consistent S3 key for chunk."""
    dataset_short = dataset_name.split('/')[-1]  # Extract last part of dataset name
    config_part = f"_{config_name}" if config_name else ""
    
    return f"{prefix}/{dataset_short}{config_part}/chunk_{chunk_index:06d}_{start_idx}_{end_idx}.parquet"


def validate_chunk_data(df: pd.DataFrame, min_records: int = 1) -> bool:
    """Validate chunk data before upload."""
    if len(df) < min_records:
        logger.warning(f"Chunk has only {len(df)} records, minimum required: {min_records}")
        return False
    
    # Check for required columns (adjust based on your dataset)
    required_columns = ['text']  # Common for text datasets
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return False
    
    # Check for empty text content
    if 'text' in df.columns:
        empty_text_count = df['text'].isna().sum()
        if empty_text_count > len(df) * 0.5:  # More than 50% empty
            logger.warning(f"Chunk has {empty_text_count} empty text records out of {len(df)}")
    
    return True


def log_memory_usage(prefix: str = ""):
    """Log current memory usage."""
    memory = psutil.virtual_memory()
    logger.info(f"{prefix}Memory: {memory.percent:.1f}% used, {memory.available / 1024**3:.1f}GB available")


def emergency_memory_cleanup():
    """Emergency memory cleanup when usage is too high."""
    memory_before = psutil.virtual_memory().percent
    gc.collect()
    time.sleep(2)
    memory_after = psutil.virtual_memory().percent
    logger.info(f"Emergency cleanup: {memory_before:.1f}% -> {memory_after:.1f}%")