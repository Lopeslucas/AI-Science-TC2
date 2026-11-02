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


def standardize_text_columns(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df = df.copy()

    for column in df.columns:
        if (
            pd.api.types.is_object_dtype(df[column])
            or pd.api.types.is_string_dtype(df[column])
        ):
            df[column] = (
                df[column]
                .astype("string")
                .str.strip()
            )

    return df


def validate_schema(
    df: pd.DataFrame,
) -> None:
    assert len(df.columns) > 0, (
        "O dicionário não possui colunas."
    )

    print(
        f"Schema OK: "
        f"{len(df.columns)} colunas"
    )


def validate_not_empty(
    df: pd.DataFrame,
) -> None:
    assert not df.empty, (
        "O dicionário está vazio."
    )

    print(
        f"Volume OK: "
        f"{len(df)} registros"
    )


def validate_fully_null_rows(
    df: pd.DataFrame,
) -> None:
    fully_null = df[
        df.isna().all(axis=1)
    ]

    assert fully_null.empty, (
        f"Foram encontradas "
        f"{len(fully_null)} linhas completamente nulas."
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


def validate_quality(
    df: pd.DataFrame,
) -> None:
    print(
        "\nExecutando validações "
        "de qualidade..."
    )

    validate_schema(df)
    validate_not_empty(df)
    validate_fully_null_rows(df)
    validate_duplicates(df)


def write_silver(
    df: pd.DataFrame,
) -> None:
    s3 = boto3.client("s3")

    buffer = BytesIO()

    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow",
        compression="snappy",
    )

    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=SILVER_KEY,
        Body=buffer.getvalue(),
    )

    print(
        f"Silver gravada: "
        f"s3://{AWS_BUCKET}/{SILVER_KEY} "
        f"({len(df)} registros)"
    )


def main():
    print(
        "Lendo Bronze Dicionário..."
    )

    df = read_parquet_from_s3(
        BRONZE_KEY
    )

    print(
        f"Registros lidos da Bronze: "
        f"{len(df)}"
    )

    print(
        "\nPadronizando colunas de texto..."
    )

    df = standardize_text_columns(df)

    validate_quality(df)

    print("\nGravando Silver...")

    write_silver(df)

    print(
        "\nTransformação Dicionário concluída."
    )


if __name__ == "__main__":
    main()