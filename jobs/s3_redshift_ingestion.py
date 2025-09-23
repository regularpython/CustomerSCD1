import psycopg2


BUCKET_SRC = "s3://batch89-pyspark/CustomerSCD1JOBS/cleaned-data/"
# -------------------------
# Environment / config
# -------------------------
# Prefer environment variables. Replace defaults or set env vars externally.
REDSHIFT_HOST = "batch89-workgroup.339713066136.us-east-1.redshift-serverless.amazonaws.com"
REDSHIFT_PORT = 5439
REDSHIFT_DB = "dev"
REDSHIFT_USER = "admin"
REDSHIFT_PASSWORD = "Admin123456789"

REDSHIFT_TABLE = "batch89.customers_stage_new"



copy_sql = f"""
COPY {REDSHIFT_TABLE}
FROM '{BUCKET_SRC}'
CREDENTIALS 'aws_iam_role=arn:aws:iam::339713066136:role/service-role/AmazonRedshift-CommandsAccessRole-20250331T133229'
FORMAT AS PARQUET;
"""


conn = None
try:
    print("Connecting to Redshift:", REDSHIFT_HOST)
    conn = psycopg2.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    conn.autocommit = True
    cur = conn.cursor()
    print("Running COPY ...")
    print('Command: ', copy_sql)
    # Step 1: Truncate staging table
    truncate_sql = f"TRUNCATE TABLE {REDSHIFT_TABLE};"
    print(f"Running TRUNCATE on {REDSHIFT_TABLE} ...")
    cur.execute(truncate_sql)

    # Step 2: Run COPY command
    print("Running COPY ...")
    print("Command:", copy_sql)
    cur.execute(copy_sql)
except Exception as e:
    print("ERROR:", str(e))
    raise
finally:
    if conn:
        conn.close()
