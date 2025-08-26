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

BUCKET_SRC = 's3a://batch89-pyspark/landing_zone/'
# Extract the data from s3
df = (spark.read
      .option("header", True)
      .option("inferSchema", True)
      .csv(BUCKET_SRC))


df.write.partitionBy("year", "genre") \
    .parquet("s3a://batch89-pyspark/destination2/", mode="overwrite")
print('Hello world!')
job.commit()