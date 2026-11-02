import argparse
from io import BytesIO

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"


TABLES = {
    "uf": {
        "bronze_prefix": "bronze/uf/",
        "silver_prefix": "silver/uf/",
        "partition_column": "ano",
        "natural_key": [
            "ano",
            "sigla_uf",
            "serie",
            "rede",
        ],
        "required_columns": [
            "ano",
            "sigla_uf",
            "serie",
            "rede",
        ],
    },

    "municipio": {
        "bronze_prefix": "bronze/municipio/",
        "silver_prefix": "silver/municipio/",
        "partition_column": "ano",
        "natural_key": [
            "ano",
            "id_municipio",
            "serie",
            "rede",
        ],
        "required_columns": [
            "ano",
            "id_municipio",
            "serie",
            "rede",
        ],
    },
}


def list_parquet_files(prefix: str) -> list[str]:
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


def extract_partition_value(
    key: str,
    partition_column: str,
) -> int:
    partition_text = key.split(
        f"{partition_column}="
    )[1].split("/")[0]

    return int(partition_text)


def read_layer(
    prefix: str,
    partition_column: str,
) -> pd.DataFrame:
    parquet_files = list_parquet_files(prefix)

    if not parquet_files:
        raise FileNotFoundError(
            f"Nenhum Parquet encontrado em "
            f"s3://{AWS_BUCKET}/{prefix}"
        )

    dataframes = []

    for key in parquet_files:
        df = read_parquet_from_s3(key)

        partition_value = extract_partition_value(
            key=key,
            partition_column=partition_column,
        )

        df[partition_column] = partition_value

        dataframes.append(df)

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def validate_total_volume(
    df_bronze: pd.DataFrame,
    df_silver: pd.DataFrame,
) -> None:
    assert len(df_bronze) == len(df_silver), (
        "\nVolume total divergente.\n"
        f"Bronze: {len(df_bronze)}\n"
        f"Silver: {len(df_silver)}"
    )

    print(
        f"Volume total OK: "
        f"{len(df_silver)} registros"
    )


def validate_partition_volume(
    df_bronze: pd.DataFrame,
    df_silver: pd.DataFrame,
    partition_column: str,
) -> None:
    bronze_counts = (
        df_bronze
        .groupby(partition_column)
        .size()
        .sort_index()
    )

    silver_counts = (
        df_silver
        .groupby(partition_column)
        .size()
        .sort_index()
    )

    assert bronze_counts.equals(silver_counts), (
        "\nVolume por partição divergente.\n"
        f"Bronze:\n{bronze_counts}\n\n"
        f"Silver:\n{silver_counts}"
    )

    print("Volume por partição OK")

    for partition_value, count in silver_counts.items():
        print(
            f"  {partition_column}={partition_value}: "
            f"{count} registros"
        )


def validate_schema(
    df_bronze: pd.DataFrame,
    df_silver: pd.DataFrame,
) -> None:
    bronze_columns = set(df_bronze.columns)
    silver_columns = set(df_silver.columns)

    assert bronze_columns == silver_columns, (
        "\nSchema lógico divergente.\n"
        f"Bronze: {sorted(bronze_columns)}\n"
        f"Silver: {sorted(silver_columns)}"
    )

    print("Schema lógico OK")


def validate_natural_key(
    df_silver: pd.DataFrame,
    natural_key: list[str],
) -> None:
    duplicated = df_silver[
        df_silver.duplicated(
            subset=natural_key,
            keep=False,
        )
    ]

    assert duplicated.empty, (
        f"\nForam encontrados "
        f"{len(duplicated)} registros duplicados "
        f"na chave natural {natural_key}."
    )

    print("Chave natural OK")

def validate_required_fields(
    df_silver: pd.DataFrame,
    required_columns: list[str],
) -> None:

    null_counts = (
        df_silver[required_columns]
        .isna()
        .sum()
    )

    invalid = null_counts[
        null_counts > 0
    ]

    assert invalid.empty, (
        "\nCampos obrigatórios com nulos:\n"
        f"{invalid}"
    )

    print("Campos obrigatórios OK")


def validate_taxa_alfabetizacao(
    df_silver: pd.DataFrame,
) -> None:
    invalid = df_silver[
        df_silver["taxa_alfabetizacao"].notna()
        & ~df_silver["taxa_alfabetizacao"].between(
            0,
            100,
        )
    ]

    assert invalid.empty, (
        f"\nExistem {len(invalid)} registros com "
        f"taxa_alfabetizacao fora de 0-100."
    )

    print("Taxa de alfabetização OK")


def validate_table(table_name: str) -> None:
    config = TABLES[table_name]

    print("=" * 60)
    print(f"Validando Silver: {table_name}")
    print("=" * 60)

    print("\nLendo Bronze...")

    df_bronze = read_layer(
        prefix=config["bronze_prefix"],
        partition_column=config["partition_column"],
    )

    print(
        f"Bronze: {len(df_bronze)} registros"
    )

    print("\nLendo Silver...")

    df_silver = read_layer(
        prefix=config["silver_prefix"],
        partition_column=config["partition_column"],
    )

    print(
        f"Silver: {len(df_silver)} registros"
    )

    print("\nExecutando reconciliação...")

    validate_total_volume(
        df_bronze=df_bronze,
        df_silver=df_silver,
    )

    validate_partition_volume(
        df_bronze=df_bronze,
        df_silver=df_silver,
        partition_column=config["partition_column"],
    )

    validate_schema(
        df_bronze=df_bronze,
        df_silver=df_silver,
    )

    print("\nExecutando validações da Silver...")

    validate_required_fields(
        df_silver=df_silver,
        required_columns=config["required_columns"],

    )

    validate_taxa_alfabetizacao(
        df_silver=df_silver,
    )

    validate_natural_key(
        df_silver=df_silver,
        natural_key=config["natural_key"],
    )

    print("\nValidação Silver concluída com sucesso.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Validação da camada Silver"
    )

    parser.add_argument(
        "table",
        choices=TABLES.keys(),
        help="Tabela Silver que será validada",
    )

    args = parser.parse_args()

    validate_table(args.table)