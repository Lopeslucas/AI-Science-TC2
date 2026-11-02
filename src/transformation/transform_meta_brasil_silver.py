from io import BytesIO
import re

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"

BRONZE_PREFIX = "bronze/meta_alfabetizacao_brasil/"
SILVER_PREFIX = "silver/meta_alfabetizacao_brasil/"


def get_meta_columns(df: pd.DataFrame) -> list[str]:
    """O horizonte é definido pelas colunas da fonte, não por uma carga histórica."""
    candidates = [c for c in df.columns if c.startswith("meta_alfabetizacao_")]
    if not candidates or any(not re.fullmatch(r"meta_alfabetizacao_[0-9]{4}", c) for c in candidates):
        raise ValueError("Colunas de metas ausentes ou com ano inválido.")
    if not df.columns.is_unique:
        raise ValueError("Colunas duplicadas na fonte.")
    return sorted(candidates)


BRONZE_BASE_COLUMNS = {
    "ano",
    "rede",
    "taxa_alfabetizacao",
    "percentual_participacao",
}


EXPECTED_SILVER_COLUMNS = {
    "safra",
    "rede",
    "taxa_alfabetizacao",
    "percentual_participacao",
    "ano_meta",
    "meta_alfabetizacao",
}


NATURAL_KEY = [
    "safra",
    "rede",
    "ano_meta",
]


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
    partition_column: str = "ano",
) -> int:
    partition_text = key.split(
        f"{partition_column}="
    )[1].split("/")[0]

    return int(partition_text)


def read_bronze_meta_brasil() -> pd.DataFrame:
    parquet_files = list_parquet_files(
        BRONZE_PREFIX
    )

    if not parquet_files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em "
            f"s3://{AWS_BUCKET}/{BRONZE_PREFIX}"
        )

    dataframes = []

    for key in parquet_files:
        df = read_parquet_from_s3(key)

        # O ano não está fisicamente dentro do Parquet.
        # Recuperamos pelo caminho ano=YYYY/
        ano = extract_partition_value(key)

        df["ano"] = ano

        dataframes.append(df)

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def validate_bronze_schema(df: pd.DataFrame) -> None:
    expected = BRONZE_BASE_COLUMNS | set(get_meta_columns(df))
    if set(df.columns) != expected:
        raise ValueError(f"Schema Bronze inesperado. Esperado: {sorted(expected)}; encontrado: {sorted(df.columns)}")


def standardize_types(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df = df.copy()

    df["ano"] = (
        pd.to_numeric(
            df["ano"],
            errors="raise",
        )
        .astype("Int64")
    )

    df["rede"] = (
        df["rede"]
        .astype("string")
        .str.strip()
    )

    numeric_columns = [
        "taxa_alfabetizacao",
        "percentual_participacao",
        *get_meta_columns(df),
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="raise",
        )

    return df


def transform_wide_to_long(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Transforma:

    ano | meta_2024 | meta_2025 | ...

    em:

    safra | ano_meta | meta_alfabetizacao
    """

    df_long = df.melt(
        id_vars=[
            "ano",
            "rede",
            "taxa_alfabetizacao",
            "percentual_participacao",
        ],
        value_vars=get_meta_columns(df),
        var_name="ano_meta",
        value_name="meta_alfabetizacao",
    )

    # ano original representa a safra/vintage
    df_long = df_long.rename(
        columns={
            "ano": "safra",
        }
    )

    # Converte:
    # meta_alfabetizacao_2024 -> 2024
    df_long["ano_meta"] = (
        df_long["ano_meta"]
        .str.extract(r"(\d{4})")[0]
        .astype("Int64")
    )

    return df_long


def validate_required_fields(
    df: pd.DataFrame,
) -> None:
    required_columns = [
        "safra",
        "rede",
        "ano_meta",
    ]

    null_counts = (
        df[required_columns]
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


def validate_meta_range(
    df: pd.DataFrame,
) -> None:
    invalid = df[
        df["meta_alfabetizacao"].notna()
        & ~df["meta_alfabetizacao"].between(
            0,
            100,
        )
    ]

    assert invalid.empty, (
        f"Foram encontradas {len(invalid)} metas "
        f"fora do intervalo 0-100."
    )


def validate_taxa_range(
    df: pd.DataFrame,
) -> None:
    invalid = df[
        df["taxa_alfabetizacao"].notna()
        & ~df["taxa_alfabetizacao"].between(
            0,
            100,
        )
    ]

    assert invalid.empty, (
        f"Foram encontrados {len(invalid)} registros com "
        f"taxa_alfabetizacao fora de 0-100."
    )


def validate_participacao_range(
    df: pd.DataFrame,
) -> None:
    invalid = df[
        df["percentual_participacao"].notna()
        & ~df["percentual_participacao"].between(
            0,
            100,
        )
    ]

    assert invalid.empty, (
        f"Foram encontrados {len(invalid)} registros com "
        f"percentual_participacao fora de 0-100."
    )


def validate_natural_key(
    df: pd.DataFrame,
) -> None:
    duplicated = df[
        df.duplicated(
            subset=NATURAL_KEY,
            keep=False,
        )
    ]

    assert duplicated.empty, (
        f"Foram encontrados "
        f"{len(duplicated)} registros duplicados "
        f"na chave natural {NATURAL_KEY}."
    )


def validate_silver_schema(
    df: pd.DataFrame,
) -> None:
    columns = set(df.columns)

    assert columns == EXPECTED_SILVER_COLUMNS, (
        "\nSchema Silver inesperado.\n"
        f"Esperado: {sorted(EXPECTED_SILVER_COLUMNS)}\n"
        f"Encontrado: {sorted(columns)}"
    )


def validate_ano_meta(df: pd.DataFrame) -> None:
    for column in ["safra", "ano_meta"]:
        values = pd.to_numeric(df[column], errors="raise")
        if values.isna().any() or not (values.gt(0) & values.mod(1).eq(0)).all():
            raise ValueError(f"{column} deve conter anos inteiros positivos, sem nulos.")


def validate_quality(
    df: pd.DataFrame,
) -> None:
    print("\nExecutando validações da Silver...")

    validate_silver_schema(df)
    print("Schema Silver OK")

    validate_required_fields(df)
    validate_ano_meta(df)
    print("Campos obrigatórios OK")

    validate_taxa_range(df)
    print("Taxa de alfabetização OK")

    validate_meta_range(df)
    print("Metas de alfabetização OK")

    validate_participacao_range(df)
    print("Percentual de participação OK")

    validate_natural_key(df)
    print("Chave natural OK")


def write_silver(
    df: pd.DataFrame,
) -> None:
    s3 = boto3.client("s3")

    # Mantemos a partição pela safra da fonte.
    for safra, df_safra in df.groupby("safra"):
        buffer = BytesIO()

        # safra fica representada no path Hive.
        df_to_write = df_safra.drop(
            columns=["safra"]
        )

        df_to_write.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
            compression="snappy",
        )

        key = (
            f"{SILVER_PREFIX}"
            f"safra={int(safra)}/"
            f"meta_alfabetizacao_brasil.parquet"
        )

        s3.put_object(
            Bucket=AWS_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
        )

        print(
            f"Silver gravada: "
            f"s3://{AWS_BUCKET}/{key} "
            f"({len(df_safra)} registros)"
        )


def main():
    files = list_parquet_files(BRONZE_PREFIX)
    if not files:
        raise FileNotFoundError(f"Nenhum Parquet em s3://{AWS_BUCKET}/{BRONZE_PREFIX}")
    parts = []
    for key in files:
        bronze = read_parquet_from_s3(key)
        bronze["ano"] = extract_partition_value(key)
        validate_bronze_schema(bronze)
        bronze = standardize_types(bronze)
        silver = transform_wide_to_long(bronze)
        expected_rows = len(bronze) * len(get_meta_columns(bronze))
        if len(silver) != expected_rows:
            raise ValueError("Volume divergente na normalização de metas.")
        validate_quality(silver)
        print(f"Fonte: {key} | Bronze: {len(bronze)} | Silver: {len(silver)}")
        parts.append(silver)
    result = pd.concat(parts, ignore_index=True)
    validate_quality(result)
    write_silver(result)


if __name__ == "__main__":
    main()