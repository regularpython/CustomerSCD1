# inspect_parquet_schema.py
import pyarrow.parquet as pq
import s3fs

s3_path = "s3://batch89-pyspark/CustomerSCD1JOBS/cleaned-data/part-00000-6352a4ad-6d6a-4790-b547-5f0c7d211cc4-c000.snappy.parquet"

fs = s3fs.S3FileSystem()   # ensure AWS creds are available in env or ~/.aws
with fs.open(s3_path.replace("s3://",""), "rb") as f:   # s3fs open needs bucket/key; if using path, pass as fs.open(s3_path)
    # alternate: pq.ParquetFile(f) works if f is file-like
    pf = pq.ParquetFile(f)
    print("Parquet schema:")
    print(pf.schema)
    print("\nRow groups:", pf.num_row_groups)
    for i in range(pf.num_row_groups):
        rg = pf.metadata.row_group(i)
        print(f" Row group {i} columns:", [rg.column(j).path_in_schema for j in range(rg.num_columns)])
