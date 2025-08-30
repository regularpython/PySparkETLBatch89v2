
import os, json, base64, sys
from pyspark.sql import SparkSession
import pydantic
import pandas

# Try Glue utils (works only in Glue job)
try:
    from awsglue.utils import getResolvedOptions
    IS_GLUE = True
except ImportError:
    IS_GLUE = False


# ---- override with Glue job args (if running in Glue) ----
if IS_GLUE:
    args = getResolvedOptions(sys.argv, ["AWS_REGION", "S3_INPUT", "S3_OUTPUT", "SECRET_NAME"])
    AWS_REGION  = args.get("AWS_REGION")
    S3_INPUT    = args.get("S3_INPUT")
    S3_OUTPUT   = args.get("S3_OUTPUT")
    SECRET_NAME = args.get("SECRET_NAME")
else:
    # ---- defaults from env (PyCharm/local) ----
    AWS_REGION  = os.getenv("AWS_REGION")
    S3_INPUT    = os.getenv("S3_INPUT")
    S3_OUTPUT   = os.getenv("S3_OUTPUT")
    SECRET_NAME = os.getenv("SECRET_NAME")  # optional


# ---- optional: load secret JSON ----
def load_secret(name):
    if not name:
        return {}
    import boto3
    sm = boto3.client("secretsmanager", region_name=AWS_REGION)
    resp = sm.get_secret_value(SecretId=name)
    raw = resp.get("SecretString") or base64.b64decode(resp["SecretBinary"]).decode()
    try: return json.loads(raw)
    except: return {"value": raw}


secret = load_secret(SECRET_NAME)
db_details = secret



# ---- spark session ----
if IS_GLUE:
    from awsglue.transforms import *
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session

else:
    PKGS = ",".join([
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    ])

    spark = (
        SparkSession.builder
        .appName("pyspark_test")
        .master("local[*]")
        # === S3A + credentials provider ===
        .config("spark.jars.packages", PKGS)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        # Optional: tune connections
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")
        .getOrCreate()
    )


# ---- read → transform → write ----
# Your code
# -------------------------------
print('Is this glue environment:', IS_GLUE)
print('AWS_REGION: ', AWS_REGION)
print('S3_INPUT: ', S3_INPUT)
print('S3_OUTPUT: ', S3_OUTPUT)
print('SECRET_NAME: ', SECRET_NAME)
print('DB Details: ', db_details)


if IS_GLUE:
    spark.stop()
