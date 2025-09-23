import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import boto3
from botocore.exceptions import ClientError
from pyspark.sql.functions import col, when
import logging

# ==============================
# Setup Logger
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("PySparkJob")
env = 'dev'
if env == 'local':

    java_home = os.environ.get("JAVA_HOME")
    print(java_home)
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]
    OUTPUT_PATH = os.environ["OUTPUT_PATH"]

    PKGS = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367"
    logger.info("Creating Spark session...")
    spark = (
        SparkSession.builder
        .appName("pyspark_test")
        .master("local[*]")
        .config("spark.jars.packages", PKGS)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")
        .config("spark.jars", r"C:\Users\Bhavani_Sai\Downloads\mysql-connector-j-9.4.0\mysql-connector-j-9.4.0\mysql-connector-j-9.4.0.jar")
        .getOrCreate()
    )
    logger.info("Spark session created successfully.")
else:
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job

    OUTPUT_PATH = "s3a://batch89-pyspark/CustomerSCD1JOBS/cleaned-data"
    ## @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    print('Hello World!')
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session



def get_secret():

    secret_name = "dev/rds/mysql"
    region_name = "us-east-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        logger.info(f"Fetching secret {secret_name} from Secrets Manager...")
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        logger.info("Secret fetched successfully.")
    except ClientError as e:
        logger.error("Failed to fetch secret from AWS Secrets Manager", exc_info=True)
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

    secret = get_secret_value_response['SecretString']
    return json.loads(secret)
    # Your code goes here.

def build_jdbc_url(secret, additional_params=None):
    """
    Build MySQL JDBC URL using secret fields.
    secret: dict expected to include host, port, database (or dbname)
    additional_params: str (like '?useSSL=false&serverTimezone=UTC')
    """
    host = secret.get("Host")
    port = secret.get("port", 3306)
    database = secret.get("database")
    if not host or not database:
        raise RuntimeError("Secret must include host and database keys.")
    url = f"jdbc:mysql://{host}:{port}/{database}"
    if additional_params:
        url = url + additional_params
    return url

# Bucket
DB_PARAMS = get_secret()
db_url = build_jdbc_url(DB_PARAMS)
logger.info(f"JDBC URL built: {db_url}")
query = """
SELECT * FROM retail.customers where customer_id<3
"""
query = """
(
  SELECT customer_id, name, email, phone, record_date
  FROM customers
) AS t
"""
# Read from JDBC
logger.info("Reading data from RDS Database...")
df = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", query) \
    .option("user", DB_PARAMS["user"]) \
    .option("password", DB_PARAMS["Password"]) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()
logger.info("Data read from RDS successfully.")
logger.info("Cleaning data (handling NULLs and empty strings)...")

df_no_empty = df.select([when(col(c) == "", None).otherwise(col(c)).alias(c) for c in df.columns])

# Drop rows where any column is NULL
df_clean = df_no_empty.dropna(how="any")
logger.info("Cleaned DataFrame after dropping NULLs and empty strings:")
df_clean = df_clean.withColumn("record_date", col("record_date").cast("string"))
df_clean.show()


# Write to Parquet partitioned by record_date
output_path = OUTPUT_PATH  # or "file:///tmp/output"
logger.info(f"Writing cleaned data to {output_path} partitioned by record_date...")
df_clean.write.mode("overwrite").parquet(output_path)

print(f"Data written successfully to {output_path} partitioned by record_date")
logger.info("Data written successfully.")