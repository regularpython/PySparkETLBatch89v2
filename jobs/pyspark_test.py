

import os

env = os.getenv('env')

import boto3
from botocore.exceptions import ClientError


def get_secret():

    secret_name = "batch89"
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
    return secret
    # Your code goes here.

# response = get_secret()
# print(response)

spark = None
if env == 'Glue':
    import sys
    from awsglue.transforms import *
    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job

    ## @params: [JOB_NAME]
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])

    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

elif env == 'Dev':
    java_home = os.environ.get("JAVA_HOME")
    from pyspark.sql import SparkSession

    # PKGS = ",".join([
    #     "org.apache.hadoop:hadoop-aws:3.3.4",
    #     "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    # ])
    spark = (
        SparkSession.builder
        .appName("pyspark_test")
        .master("local[*]")
        # === S3A + credentials provider ===
        # .config("spark.jars.packages", PKGS)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        # Optional: tune connections
        .config("spark.hadoop.fs.s3a.connection.maximum", "200")
        .getOrCreate()
    )

if spark:
    data = [("Sairam", 28), ("Anil", 25)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)

    # BUCKET_SRC = 's3a://batch89-pyspark/landing_zone/'
    # # Extract the data from s3
    # df = (spark.read
    #       .option("header", True)
    #       .option("inferSchema", True)
    #       .csv(BUCKET_SRC))
    #
    #
    # df.write.partitionBy("year", "genre") \
    #     .parquet("s3a://batch89-pyspark/destination3/", mode="overwrite")
    # print('Hello world!')
    # print('Hi this is sairam!')
    # # job.commit()