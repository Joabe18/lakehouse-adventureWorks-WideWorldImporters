import os
from dotenv import load_dotenv
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

load_dotenv()


def run_quality_gate(
    df: DataFrame,
    table_name: str,
    not_null_cols: list[str],
    positive_cols: list[str] = None,
    min_row_count: int = 1
) -> DataFrame:
    total_rows = df.count()
    print(f"\n[Data Quality - {table_name}] Total de registros recebidos: {total_rows}")

    if total_rows < min_row_count:
        raise ValueError(
            f"CIRCUIT BREAKER ACIONADO em '{table_name}': "
            f"Volume ({total_rows}) abaixo do mínimo esperado ({min_row_count})."
        )

    positive_cols = positive_cols or []
    error_reasons = []

    # Regra 1: Verificação de Nulos ou Strings Vazias (completude)
    for col_name in not_null_cols:
        condition = F.col(col_name).isNull() | (F.trim(F.col(col_name).cast("string")) == "")
        error_reasons.append(
            F.when(condition, F.lit(f"NULO_OU_VAZIO:{col_name}"))
        )

    # Regra 2: Verificação de Valores Positivos (validade / distribuição)
    for col_name in positive_cols:
        condition = F.col(col_name).isNull() | (F.col(col_name) <= 0)
        error_reasons.append(
            F.when(condition, F.lit(f"VALOR_INVALIDO:{col_name}"))
        )

    # Concatena todos os motivos de erro encontrados em uma única coluna de auditoria
    df_evaluated = df.withColumn("_dq_errors", F.concat_ws(" | ", *error_reasons))

    df_valid = df_evaluated.filter(F.col("_dq_errors") == "").drop("_dq_errors")
    df_quarantine = (
        df_evaluated.filter(F.col("_dq_errors") != "")
        .withColumn("_quarantine_timestamp", F.current_timestamp())
    )

    quarantine_count = df_quarantine.count()
    valid_count = df_valid.count()

    if quarantine_count > 0:
        bucket = os.getenv("S3_BUCKET_NAME")
        quarantine_path = f"s3a://{bucket}/quarantine/{table_name}"
        df_quarantine.write.mode("append").parquet(quarantine_path)
        print(f"[ALERTA] {quarantine_count} registros enviados para quarentena em: {quarantine_path}")

    print(f"[OK] {valid_count} registros aprovados para a camada Silver.")
    return df_valid
