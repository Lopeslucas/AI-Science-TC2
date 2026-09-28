import argparse
from io import BytesIO

import boto3
import pandas as pd
from google.cloud import bigquery

import os
import tempfile

import pyarrow.parquet as pq


GCP_PROJECT_ID = "angular-harmony-509500-m1"
AWS_BUCKET = "ai-science-tc2"

DATASET = "basedosdados.br_inep_avaliacao_alfabetizacao"


TABLES = {
    "uf": {
        "source": f"{DATASET}.uf",
        "partition_column": "ano",
        "large_table": False,
    },
    "municipio": {
        "source": f"{DATASET}.municipio",
        "partition_column": "ano",
        "large_table": False,
    },
    "meta_alfabetizacao_brasil": {
        "source": f"{DATASET}.meta_alfabetizacao_brasil",
        "partition_column": "ano",
        "large_table": False,
    },
    "meta_alfabetizacao_uf": {
        "source": f"{DATASET}.meta_alfabetizacao_uf",
        "partition_column": "ano",
        "large_table": False,
    },
    "meta_alfabetizacao_municipio": {
        "source": f"{DATASET}.meta_alfabetizacao_municipio",
        "partition_column": "ano",
        "large_table": False,
    },
    "dicionario": {
        "source": f"{DATASET}.dicionario",
        "partition_column": None,
        "large_table": False,
    },
    "alunos": {
        "source": f"{DATASET}.alunos",
        "partition_column": "ano",
        "large_table": True,
    },
}


def get_bigquery_dataframe(source_table: str) -> pd.DataFrame:
    client = bigquery.Client(project=GCP_PROJECT_ID)

    query = f"""
    SELECT *
    FROM `{source_table}`
    """

    return client.query(query).to_dataframe()


def list_s3_parquet_files(prefix: str) -> list[str]:
    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")

    keys = []

    for page in paginator.paginate(
        Bucket=AWS_BUCKET,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(".parquet"):
                keys.append(key)

    return keys


def read_parquet_from_s3(key: str) -> pd.DataFrame:
    s3 = boto3.client("s3")

    response = s3.get_object(
        Bucket=AWS_BUCKET,
        Key=key,
    )

    return pd.read_parquet(
        BytesIO(response["Body"].read())
    )


def get_s3_dataframe(table_name: str) -> pd.DataFrame:
    prefix = f"bronze/{table_name}/"

    parquet_files = list_s3_parquet_files(prefix)

    if not parquet_files:
        raise FileNotFoundError(
            f"Nenhum arquivo Parquet encontrado em "
            f"s3://{AWS_BUCKET}/{prefix}"
        )

    dataframes = []

    for key in parquet_files:
        df = read_parquet_from_s3(key)
        dataframes.append(df)

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def validate_schema(
    df_source: pd.DataFrame,
    df_bronze: pd.DataFrame,
) -> None:
    source_columns = set(df_source.columns)
    bronze_columns = set(df_bronze.columns)

    assert source_columns == bronze_columns, (
        "\nSchema divergente.\n"
        f"Origem: {sorted(source_columns)}\n"
        f"Bronze: {sorted(bronze_columns)}"
    )


def validate_total_volume(
    df_source: pd.DataFrame,
    df_bronze: pd.DataFrame,
) -> None:
    assert len(df_source) == len(df_bronze), (
        "\nVolume divergente.\n"
        f"Origem: {len(df_source)}\n"
        f"Bronze: {len(df_bronze)}"
    )


def validate_partitions(
    df_source: pd.DataFrame,
    df_bronze: pd.DataFrame,
    partition_column: str,
) -> None:
    source_counts = (
        df_source
        .groupby(partition_column)
        .size()
        .sort_index()
    )

    bronze_counts = (
        df_bronze
        .groupby(partition_column)
        .size()
        .sort_index()
    )

    assert source_counts.equals(bronze_counts), (
        "\nContagem por partição divergente.\n"
        f"Origem:\n{source_counts}\n\n"
        f"Bronze:\n{bronze_counts}"
    )

    print("\nContagem por partição:")

    for partition_value, count in source_counts.items():
        print(
            f"{partition_column}={partition_value}: "
            f"{count} registros"
        )


def validate_table(table_name: str) -> None:
    config = TABLES[table_name]

    if config.get("large_table", False):
        print("=" * 60)
        print(f"Validando tabela grande: {table_name}")
        print("=" * 60)

        validate_large_table(
            table_name=table_name,
            source_table=config["source"],
            partition_column=config["partition_column"],
        )

        print("\nValidação concluída com sucesso.")
        return

    print("=" * 60)
    print(f"Validando tabela: {table_name}")
    print("=" * 60)

    print("\nLendo origem no BigQuery...")

    df_source = get_bigquery_dataframe(
        config["source"]
    )

    print(
        f"Origem: {len(df_source)} registros"
    )

    print("\nLendo Bronze no S3...")

    df_bronze = get_s3_dataframe(
        table_name
    )

    print(
        f"Bronze: {len(df_bronze)} registros"
    )

    print("\nValidando volume...")

    validate_total_volume(
        df_source=df_source,
        df_bronze=df_bronze,
    )

    print("Volume OK")

    print("\nValidando schema...")

    validate_schema(
        df_source=df_source,
        df_bronze=df_bronze,
    )

    print("Schema OK")

    partition_column = config["partition_column"]

    if partition_column:
        print("\nValidando partições...")

        validate_partitions(
            df_source=df_source,
            df_bronze=df_bronze,
            partition_column=partition_column,
        )

        print("Partições OK")

    print("\nValidação concluída com sucesso.")


def get_bigquery_partition_counts(
    source_table: str,
    partition_column: str,
) -> dict:
    client = bigquery.Client(project=GCP_PROJECT_ID)

    query = f"""
    SELECT
        {partition_column},
        COUNT(*) AS total
    FROM `{source_table}`
    GROUP BY {partition_column}
    ORDER BY {partition_column}
    """

    df = client.query(query).to_dataframe()

    return {
        row[partition_column]: row["total"]
        for _, row in df.iterrows()
    }


def get_parquet_metadata_from_s3(key: str):
    s3 = boto3.client("s3")

    with tempfile.NamedTemporaryFile(
        suffix=".parquet",
        delete=False,
    ) as temp_file:

        temp_path = temp_file.name

    try:
        s3.download_file(
            AWS_BUCKET,
            key,
            temp_path,
        )

        parquet_file = pq.ParquetFile(temp_path)

        row_count = parquet_file.metadata.num_rows
        columns = set(parquet_file.schema_arrow.names)

        return row_count, columns

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def validate_large_table(
    table_name: str,
    source_table: str,
    partition_column: str,
) -> None:

    print("\nObtendo contagens da origem...")

    source_counts = get_bigquery_partition_counts(
        source_table=source_table,
        partition_column=partition_column,
    )

    prefix = f"bronze/{table_name}/"

    parquet_files = list_s3_parquet_files(prefix)

    if not parquet_files:
        raise FileNotFoundError(
            f"Nenhum Parquet encontrado em "
            f"s3://{AWS_BUCKET}/{prefix}"
        )

    bronze_counts = {}
    bronze_columns = None

    print("Lendo metadados dos Parquets...")

    for key in parquet_files:
        row_count, columns = get_parquet_metadata_from_s3(key)

        partition_text = key.split(
            f"{partition_column}="
        )[1].split("/")[0]

        partition_value = int(partition_text)

        bronze_counts[partition_value] = (
            bronze_counts.get(partition_value, 0)
            + row_count
        )

        if bronze_columns is None:
            bronze_columns = columns
        else:
            assert bronze_columns == columns, (
                f"Schema divergente entre arquivos: {key}"
            )

    print("\nReconciliação por partição:")

    for partition_value in sorted(source_counts):
        source_total = source_counts[partition_value]
        bronze_total = bronze_counts.get(
            partition_value,
            0,
        )

        print(
            f"{partition_column}={partition_value} | "
            f"origem={source_total} | "
            f"bronze={bronze_total}"
        )

        assert source_total == bronze_total, (
            f"Volume divergente em "
            f"{partition_column}={partition_value}"
        )

    source_total = sum(source_counts.values())
    bronze_total = sum(bronze_counts.values())

    assert source_total == bronze_total

    print(f"\nOrigem total: {source_total}")
    print(f"Bronze total: {bronze_total}")
    print("Volume OK")


    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validação genérica da camada Bronze"
    )

    parser.add_argument(
        "table",
        choices=TABLES.keys(),
        help="Tabela Bronze que será validada",
    )

    args = parser.parse_args()

    validate_table(args.table)