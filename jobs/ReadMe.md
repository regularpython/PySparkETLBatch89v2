# 🚀 PySpark ETL Project – Environment Variables Setup

This project shows how to configure **environment variables** for running a PySpark ETL job in both **local (PyCharm)** and **AWS Glue** environments, with optional integration to **AWS Secrets Manager**.

---

## ⚙️ Environment Variables

Set the following when running locally (PyCharm / terminal):

```env
AWS_REGION=us-east-1;
JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.28.6-hotspot;
PYSPARK_DRIVER_PYTHON=D:\Code\Original\05-04-2025\PySparkETLBatch89v2\Scripts\python.exe;
PYSPARK_PYTHON=D:\Code\Original\05-04-2025\PySparkETLBatch89v2\Scripts\python.exe;
PYTHONUNBUFFERED=1;
SECRET_NAME=batch89;
```

# 🚀 PySpark ETL Project

This project uses **PySpark** with AWS services integration.  
Dependencies are managed via `requirements.txt`.

---

## 📦 Requirements

The following Python packages are required:

```txt
boto3==1.40.17
botocore==1.40.17
jmespath==1.0.1
py4j==0.10.9.5
pyspark==3.3.2
python-dateutil==2.9.0.post0
s3transfer==0.13.1
six==1.17.0
urllib3==2.5.0
