import os
from dotenv import load_dotenv
from pyspark.sql import functions as F
from src.spark_session import get_spark_session
from src.quality.checks import run_quality_gate

load_dotenv()


def build_dim_clientes(spark, bucket: str):
    print("\n--- Processando dim_clientes ---")
    # 1. Lendo tabelas do AdventureWorks
    aw_cust = spark.read.parquet(f"s3a://{bucket}/bronze/adventureworks/sales_customer")
    aw_person = spark.read.parquet(f"s3a://{bucket}/bronze/adventureworks/person_person")
    aw_store = spark.read.parquet(f"s3a://{bucket}/bronze/adventureworks/sales_store")

    df_aw_clientes = (
        aw_cust.alias("c")
        .join(aw_person.alias("p"), F.col("c.PersonID") == F.col("p.BusinessEntityID"), "left")
        .join(aw_store.alias("s"), F.col("c.StoreID") == F.col("s.BusinessEntityID"), "left")
        .select(
            F.concat(F.lit("AW-"), F.col("c.CustomerID")).alias("cliente_sk"),
            F.col("c.CustomerID").cast("string").alias("id_origem"),
            F.lit("adventureworks").alias("sistema_origem"),
            F.coalesce(
                F.col("s.Name"),
                F.concat_ws(" ", F.col("p.FirstName"), F.col("p.LastName"))
            ).alias("nome_cliente"),
            F.when(F.col("c.StoreID").isNotNull(), F.lit("B2B_Loja")).otherwise(F.lit("B2C_Pessoa")).alias("segmento"),
            F.col("c._ingestion_timestamp")
        )
    )

    # 2. Lendo tabela do Wide World Importers
    wwi_cust = spark.read.parquet(f"s3a://{bucket}/bronze/wwi/sales_customers")

    df_wwi_clientes = wwi_cust.select(
        F.concat(F.lit("WWI-"), F.col("CustomerID")).alias("cliente_sk"),
        F.col("CustomerID").cast("string").alias("id_origem"),
        F.lit("wwi").alias("sistema_origem"),
        F.col("CustomerName").alias("nome_cliente"),
        F.lit("B2B_Atacado").alias("segmento"),
        F.col("_ingestion_timestamp")
    )

    # 3. União e Deduplicação Idempotente
    df_unificado = df_aw_clientes.unionByName(df_wwi_clientes).dropDuplicates(["cliente_sk"])

    # 4. Data Quality Gate
    df_clean = run_quality_gate(
        df=df_unificado,
        table_name="dim_clientes",
        not_null_cols=["cliente_sk", "nome_cliente"],
        min_row_count=100
    )

    # 5. Gravação em formato Apache Iceberg
    df_clean.writeTo("iceberg_catalog.silver_dim_clientes").createOrReplace()
    print("Tabela Iceberg 'silver_dim_clientes' gravada com sucesso!")


def build_dim_produtos(spark, bucket: str):
    print("\n--- Processando dim_produtos ---")
    aw_prod = spark.read.parquet(f"s3a://{bucket}/bronze/adventureworks/production_product")
    df_aw_prod = aw_prod.select(
        F.concat(F.lit("AW-"), F.col("ProductID")).alias("produto_sk"),
        F.col("ProductID").cast("string").alias("id_origem"),
        F.lit("adventureworks").alias("sistema_origem"),
        F.col("Name").alias("nome_produto"),
        F.coalesce(F.col("Color"), F.lit("Não Informado")).alias("cor"),
        F.col("ListPrice").cast("double").alias("preco_unitario_padrao"),
        F.col("_ingestion_timestamp")
    )

    wwi_prod = spark.read.parquet(f"s3a://{bucket}/bronze/wwi/warehouse_stockitems")
    df_wwi_prod = wwi_prod.select(
        F.concat(F.lit("WWI-"), F.col("StockItemID")).alias("produto_sk"),
        F.col("StockItemID").cast("string").alias("id_origem"),
        F.lit("wwi").alias("sistema_origem"),
        F.col("StockItemName").alias("nome_produto"),
        F.lit("Não Informado").alias("cor"),
        F.col("UnitPrice").cast("double").alias("preco_unitario_padrao"),
        F.col("_ingestion_timestamp")
    )

    df_unificado = df_aw_prod.unionByName(df_wwi_prod).dropDuplicates(["produto_sk"])

    df_clean = run_quality_gate(
        df=df_unificado,
        table_name="dim_produtos",
        not_null_cols=["produto_sk", "nome_produto"],
        min_row_count=50
    )

    df_clean.writeTo("iceberg_catalog.silver_dim_produtos").createOrReplace()
    print("Tabela Iceberg 'silver_dim_produtos' gravada com sucesso!")


def build_fato_vendas(spark, bucket: str):
    print("\n--- Processando fato_vendas ---")
    # Vendas AdventureWorks (Cabeçalho + Detalhe do Item)
    aw_header = spark.read.parquet(f"s3a://{bucket}/bronze/adventureworks/sales_salesorderheader")
    aw_detail = spark.read.parquet(f"s3a://{bucket}/bronze/adventureworks/sales_salesorderdetail")

    df_aw_vendas = (
        aw_detail.alias("d")
        .join(aw_header.alias("h"), "SalesOrderID", "inner")
        .select(
            F.concat(F.lit("AW-"), F.col("d.SalesOrderDetailID")).alias("venda_item_sk"),
            F.concat(F.lit("AW-"), F.col("h.SalesOrderID")).alias("pedido_sk"),
            F.concat(F.lit("AW-"), F.col("h.CustomerID")).alias("cliente_sk"),
            F.concat(F.lit("AW-"), F.col("d.ProductID")).alias("produto_sk"),
            F.lit("adventureworks").alias("sistema_origem"),
            F.to_date(F.col("h.OrderDate")).alias("data_pedido"),
            F.col("d.OrderQty").cast("int").alias("quantidade"),
            F.col("d.UnitPrice").cast("double").alias("preco_unitario"),
            (F.col("d.OrderQty") * F.col("d.UnitPrice")).cast("double").alias("valor_total"),
            F.col("d._ingestion_timestamp")
        )
    )

    # Vendas Wide World Importers (Pedidos + Linhas do Pedido)
    wwi_orders = spark.read.parquet(f"s3a://{bucket}/bronze/wwi/sales_orders")
    wwi_lines = spark.read.parquet(f"s3a://{bucket}/bronze/wwi/sales_orderlines")

    df_wwi_vendas = (
        wwi_lines.alias("l")
        .join(wwi_orders.alias("o"), "OrderID", "inner")
        .select(
            F.concat(F.lit("WWI-"), F.col("l.OrderLineID")).alias("venda_item_sk"),
            F.concat(F.lit("WWI-"), F.col("o.OrderID")).alias("pedido_sk"),
            F.concat(F.lit("WWI-"), F.col("o.CustomerID")).alias("cliente_sk"),
            F.concat(F.lit("WWI-"), F.col("l.StockItemID")).alias("produto_sk"),
            F.lit("wwi").alias("sistema_origem"),
            F.to_date(F.col("o.OrderDate")).alias("data_pedido"),
            F.col("l.Quantity").cast("int").alias("quantidade"),
            F.col("l.UnitPrice").cast("double").alias("preco_unitario"),
            (F.col("l.Quantity") * F.col("l.UnitPrice")).cast("double").alias("valor_total"),
            F.col("l._ingestion_timestamp")
        )
    )

    df_unificado = df_aw_vendas.unionByName(df_wwi_vendas).dropDuplicates(["venda_item_sk"])

    # Injetando 2 registros corrompidos simulados para validar o Circuit Breaker e a Quarentena
    dirty_data = [
        ("ERR-001", "ERR-PED-1", None, "AW-776", "simulacao_erro", None, -5, 100.0, -500.0, None),
        ("ERR-002", "ERR-PED-2", "WWI-1", "WWI-10", "simulacao_erro", None, 2, -50.0, -100.0, None)
    ]
    df_dirty = spark.createDataFrame(dirty_data, schema=df_unificado.schema)
    df_com_anomalias = df_unificado.unionByName(df_dirty)

    # Data Quality Gate (vai barrar os 2 registros sujos e salvar na pasta quarantine/)
    df_clean = run_quality_gate(
        df=df_com_anomalias,
        table_name="fato_vendas",
        not_null_cols=["venda_item_sk", "cliente_sk", "produto_sk", "data_pedido"],
        positive_cols=["quantidade", "preco_unitario"],
        min_row_count=1000
    )

    df_clean.writeTo("iceberg_catalog.silver_fato_vendas").createOrReplace()
    print("Tabela Iceberg 'silver_fato_vendas' gravada com sucesso!")


def main():
    bucket = os.getenv("S3_BUCKET_NAME")
    spark = get_spark_session("Transformacao_Silver_Iceberg")

    build_dim_clientes(spark, bucket)
    build_dim_produtos(spark, bucket)
    build_fato_vendas(spark, bucket)

    spark.stop()
    print("\nPipeline da Camada Silver (Apache Iceberg) concluído com sucesso!")


if __name__ == "__main__":
    main()
