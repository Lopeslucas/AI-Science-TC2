from io import BytesIO

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"

BRONZE_KEY = "bronze/dicionario/dicionario.parquet"
SILVER_KEY = "silver/dicionario/dicionario.parquet"


def read_parquet_from_s3(
    key: str,
) -> pd.DataFrame:
    s3 = boto3.client("s3")

    response = s3.get_object(
        Bucket=AWS_BUCKET,
        Key=key,
    )

    return pd.read_parquet(
        BytesIO(response["Body"].read())
    )


def validate_volume(
    df_bronze: pd.DataFrame,
    df_silver: pd.DataFrame,
) -> None:
    assert len(df_bronze) == len(df_silver), (
        "\nVolume divergente.\n"
        f"Bronze: {len(df_bronze)}\n"
        f"Silver: {len(df_silver)}"
    )

    print(f"Volume reconciliado: {len(df_silver)} registros")


def validate_schema(
    df_bronze: pd.DataFrame,
    df_silver: pd.DataFrame,
) -> None:
    bronze_columns = set(df_bronze.columns)
    silver_columns = set(df_silver.columns)

    assert bronze_columns == silver_columns, (
        "\nSchema divergente.\n"
        f"Bronze: {sorted(bronze_columns)}\n"
        f"Silver: {sorted(silver_columns)}"
    )

    print(f"Schema reconciliado: {len(df_silver.columns)} colunas")


def validate_fully_null_rows(
    df: pd.DataFrame,
) -> None:
    fully_null = df[
        df.isna().all(axis=1)
    ]

    assert fully_null.empty, (
        f"Foram encontradas "
        f"{len(fully_null)} linhas totalmente nulas."
    )

    print("Linhas completamente nulas: 0")


def validate_duplicates(
    df: pd.DataFrame,
) -> None:
    duplicated = df[
        df.duplicated(
            keep=False
        )
    ]

    assert duplicated.empty, (
        f"Foram encontrados "
        f"{len(duplicated)} registros duplicados."
    )

    print("Duplicidades completas: 0")


def main():
    print("=" * 60)
    print("Validando Silver: dicionario")
    print("=" * 60)

    print("\nLendo Bronze...")

    df_bronze = read_parquet_from_s3(
        BRONZE_KEY
    )

    print(
        f"Bronze: {len(df_bronze)} registros"
    )

    print("\nLendo Silver...")

    df_silver = read_parquet_from_s3(
        SILVER_KEY
    )

    print(
        f"Silver: {len(df_silver)} registros"
    )

    print("\nExecutando validações...")

    validate_volume(
        df_bronze=df_bronze,
        df_silver=df_silver,
    )

    validate_schema(
        df_bronze=df_bronze,
        df_silver=df_silver,
    )

    validate_fully_null_rows(
        df_silver
    )

    validate_duplicates(
        df_silver
    )

    print(
        "\nValidação Silver concluída com sucesso."
    )


if __name__ == "__main__":
    main()