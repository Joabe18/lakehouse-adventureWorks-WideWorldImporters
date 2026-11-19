import os
import duckdb
from dotenv import load_dotenv

load_dotenv()


def get_duckdb_s3_connection(db_path: str = "gold_dw.duckdb"):
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION", "us-east-1")

    con = duckdb.connect(db_path)

    # Instala e carrega extensões nativas para S3 e Apache Iceberg
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")

    # Configura credenciais da AWS na sessão do DuckDB
    con.execute(f"""
        CREATE SECRET s3_lakehouse_secret (
            TYPE S3,
            KEY_ID '{aws_key}',
            SECRET '{aws_secret}',
            REGION '{aws_region}'
        );
    """)
    return con


def build_gold_layer():
    bucket = os.getenv("S3_BUCKET_NAME")
    con = get_duckdb_s3_connection("gold_dw.duckdb")

    print("1. Lendo tabelas Apache Iceberg da Silver no S3...")
    con.execute(f"""
        CREATE OR REPLACE VIEW stg_clientes AS
        SELECT * FROM iceberg_scan('s3://{bucket}/silver/silver_dim_clientes', allow_moved_paths = true);

        CREATE OR REPLACE VIEW stg_produtos AS
        SELECT * FROM iceberg_scan('s3://{bucket}/silver/silver_dim_produtos', allow_moved_paths = true);

        CREATE OR REPLACE VIEW stg_vendas AS
        SELECT * FROM iceberg_scan('s3://{bucket}/silver/silver_fato_vendas', allow_moved_paths = true);
    """)

    print("2. Construindo modelo dimensional (Star Schema) no DuckDB...")
    # Dimensão Clientes
    con.execute("""
        CREATE OR REPLACE TABLE gold_dim_clientes AS
        SELECT
            cliente_sk,
            id_origem,
            sistema_origem,
            nome_cliente,
            segmento
        FROM stg_clientes;
    """)

    # Dimensão Produtos
    con.execute("""
        CREATE OR REPLACE TABLE gold_dim_produtos AS
        SELECT
            produto_sk,
            id_origem,
            sistema_origem,
            nome_produto,
            cor,
            preco_unitario_padrao
        FROM stg_produtos;
    """)

    # Dimensão Tempo (gerada dinamicamente a partir das datas de pedido)
    con.execute("""
        CREATE OR REPLACE TABLE gold_dim_tempo AS
        SELECT DISTINCT
            data_pedido AS data_id,
            EXTRACT(YEAR FROM data_pedido) AS ano,
            EXTRACT(MONTH FROM data_pedido) AS mes,
            EXTRACT(DAY FROM data_pedido) AS dia,
            EXTRACT(DOW FROM data_pedido) AS dia_semana,
            EXTRACT(QUARTER FROM data_pedido) AS trimestre
        FROM stg_vendas
        WHERE data_pedido IS NOT NULL;
    """)

    # Tabela Fato Vendas
    con.execute("""
        CREATE OR REPLACE TABLE gold_fato_vendas AS
        SELECT
            venda_item_sk,
            pedido_sk,
            cliente_sk,
            produto_sk,
            data_pedido AS data_id,
            sistema_origem,
            quantidade,
            preco_unitario,
            valor_total
        FROM stg_vendas;
    """)

    print("3. Criando One Big Table (OBT) pronta para Machine Learning...")
    con.execute("""
        CREATE OR REPLACE TABLE gold_obt_vendas_ml AS
        SELECT
            f.venda_item_sk,
            f.pedido_sk,
            f.sistema_origem,
            f.data_id AS data_pedido,
            t.ano,
            t.mes,
            t.trimestre,
            t.dia_semana,
            c.cliente_sk,
            c.nome_cliente,
            c.segmento AS segmento_cliente,
            p.produto_sk,
            p.nome_produto,
            p.cor AS cor_produto,
            p.preco_unitario_padrao,
            f.quantidade,
            f.preco_unitario AS preco_praticado,
            (f.preco_unitario - p.preco_unitario_padrao) AS diferenca_preco_tabela,
            f.valor_total
        FROM gold_fato_vendas f
        LEFT JOIN gold_dim_clientes c ON f.cliente_sk = c.cliente_sk
        LEFT JOIN gold_dim_produtos p ON f.produto_sk = p.produto_sk
        LEFT JOIN gold_dim_tempo t ON f.data_id = t.data_id;
    """)

    print("4. Exportando tabelas Gold em Parquet para o S3...")
    gold_tables = [
        "gold_dim_clientes",
        "gold_dim_produtos",
        "gold_dim_tempo",
        "gold_fato_vendas",
        "gold_obt_vendas_ml"
    ]

    for table in gold_tables:
        s3_out = f"s3://{bucket}/gold/{table}.parquet"
        con.execute(f"COPY {table} TO '{s3_out}' (FORMAT PARQUET);")
        row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"[OK] {table} ({row_count} linhas) -> {s3_out}")

    con.close()
    print("\nCamada Gold concluída com sucesso no DuckDB e no Amazon S3!")


if __name__ == "__main__":
    build_gold_layer()
