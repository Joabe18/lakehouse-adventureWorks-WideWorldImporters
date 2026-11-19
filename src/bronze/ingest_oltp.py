import os
from dotenv import load_dotenv
from pyspark.sql import functions as F
from src.spark_session import get_spark_session

load_dotenv()


def get_jdbc_url(database: str) -> str:
    host = os.getenv("MSSQL_HOST", "localhost")
    port = os.getenv("MSSQL_PORT", "1434")
    return f"jdbc:sqlserver://{host}:{port};databaseName={database};encrypt=true;trustServerCertificate=true;"


def list_all_tables(spark, database: str):
    jdbc_url = get_jdbc_url(database)
    user = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASSWORD")

    query = """
    (
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME
        FROM INFORMATION_SCHEMA.TABLES t
        WHERE t.TABLE_TYPE = 'BASE TABLE'
          AND t.TABLE_NAME NOT LIKE 'sys%'
          AND t.TABLE_NAME NOT LIKE '%_Archive'
    ) AS tables_list
    """
    df_tables = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
    )
    return [(row.TABLE_SCHEMA, row.TABLE_NAME) for row in df_tables.collect()]


def build_safe_query(spark, database: str, schema: str, table: str) -> str:
    jdbc_url = get_jdbc_url(database)
    user = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASSWORD")

    col_query = f"""
    (
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
    ) AS cols
    """
    cols = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", col_query)
        .option("user", user)
        .option("password", password)
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
        .load()
        .collect()
    )

    select_exprs = []
    for c in cols:
        col_name = f"[{c.COLUMN_NAME}]"
        if c.DATA_TYPE in ("geography", "geometry", "hierarchyid", "xml", "sql_variant"):
            select_exprs.append(f"CAST({col_name} AS NVARCHAR(MAX)) AS {col_name}")
        else:
            select_exprs.append(col_name)

    cols_sql = ", ".join(select_exprs)
    return f"(SELECT {cols_sql} FROM [{schema}].[{table}]) AS src"


def ingest_database(spark, database: str, source_tag: str):
    jdbc_url = get_jdbc_url(database)
    user = os.getenv("MSSQL_USER", "sa")
    password = os.getenv("MSSQL_PASSWORD")
    bucket = os.getenv("S3_BUCKET_NAME")

    tables = list_all_tables(spark, database)
    print(f"\nIniciando migração full de {len(tables)} tabelas do banco {database}...")

    for schema, table in tables:
        full_name = f"{schema}.{table}"
        try:
            safe_query = build_safe_query(spark, database, schema, table)
            df = (
                spark.read.format("jdbc")
                .option("url", jdbc_url)
                .option("dbtable", safe_query)
                .option("user", user)
                .option("password", password)
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load()
            )

            df_bronze = (
                df.withColumn("_source_system", F.lit(source_tag))
                .withColumn("_source_table", F.lit(full_name))
                .withColumn("_ingestion_timestamp", F.current_timestamp())
            )

            clean_name = f"{schema}_{table}".lower()
            output_path = f"s3a://{bucket}/bronze/{source_tag}/{clean_name}"
            df_bronze.write.mode("overwrite").parquet(output_path)
            print(f"[OK] {full_name} -> {output_path}")
        except Exception as e:
            print(f"[ERRO] Falha ao extrair {full_name}: {e}")


def main():
    spark = get_spark_session("Migracao_Full_Bronze_AW_WWI")
    ingest_database(spark, "AdventureWorks", "adventureworks")
    ingest_database(spark, "WideWorldImporters", "wwi")
    spark.stop()
    print("\nMigração completa da Camada Bronze finalizada!")


if __name__ == "__main__":
    main()
