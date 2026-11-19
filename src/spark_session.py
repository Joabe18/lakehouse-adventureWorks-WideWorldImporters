import os 
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

def get_spark_session(app_name: str) -> SparkSession:
    bucket_name = os.getenv("S3_BUCKET_NAME")
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    packages = [
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
        "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre11",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
    ]

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.memory", "4g")
        .config("spark.jars.packages", ",".join(packages))
        # configurações S3A
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        # configurações do Catáogo Apache Iceberg no S3
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
        .config("spark.sql.catalog.iceberg_catalog.warehouse", f"s3a://{bucket_name}/silver")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark