import argparse
import json

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os
import boto3
from botocore.exceptions import ClientError

java_home = os.environ.get("JAVA_HOME")
print(java_home)
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ["PATH"]


PKGS = "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.367"

spark = (
    SparkSession.builder
    .appName("pyspark_test")
    .master("local[*]")
    .config("spark.jars.packages", PKGS)
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    .config("spark.jars", r"C:\Users\Bhavani_Sai\Downloads\mysql-connector-j-9.4.0\mysql-connector-j-9.4.0\mysql-connector-j-9.4.0.jar")
    .config("spark.hadoop.fs.s3a.connection.maximum", "200")
    .getOrCreate()
)


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
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
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
print(type(DB_PARAMS))
db_url = build_jdbc_url(DB_PARAMS)
print(db_url)



# Read from JDBC
print('Reading data from RDS Database')
df = spark.read.format("jdbc") \
    .option("url", db_url) \
    .option("dbtable","customers") \
    .option("user", DB_PARAMS["user"]) \
    .option("password", DB_PARAMS["Password"]) \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .load()

df.show()