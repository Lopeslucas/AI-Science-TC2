from io import BytesIO

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"

SILVER_PREFIX = "silver/meta_alfabetizacao_brasil/"


EXPECTED_COLUMNS = {
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


def extract_safra(key: str) -> int:
    safra_text = key.split(
        "safra="
    )[1].split("/")[0]

    return int(safra_text)


def read_silver() -> pd.DataFrame:
    parquet_files = list_parquet_files(
        SILVER_PREFIX
    )

    if not parquet_files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em "
            f"s3://{AWS_BUCKET}/{SILVER_PREFIX}"
        )

    dataframes = []

    for key in parquet_files:
        df = read_parquet_from_s3(key)

        safra = extract_safra(key)

        # Reconstrói a coluna lógica da partição
        df["safra"] = safra

        dataframes.append(df)

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def report_total_volume(df: pd.DataFrame) -> None:
    print(f"Registros Silver: {len(df)}")


def report_partition_volume(df: pd.DataFrame) -> None:
    print("Registros por safra:", df.groupby("safra", dropna=False).size().to_dict())


def validate_schema(df: pd.DataFrame) -> None:
    columns = set(df.columns)

    assert columns == EXPECTED_COLUMNS, (
        "\nSchema divergente.\n"
        f"Esperado: {sorted(EXPECTED_COLUMNS)}\n"
        f"Encontrado: {sorted(columns)}"
    )

    print("Schema OK")


def validate_ano_meta(df: pd.DataFrame) -> None:
    for column in ["safra", "ano_meta"]:
        values = pd.to_numeric(df[column], errors="raise")
        if values.isna().any() or not (values.gt(0) & values.mod(1).eq(0)).all():
            raise ValueError(f"{column} deve conter anos inteiros positivos, sem nulos.")
    if df[NATURAL_KEY].isna().any().any():
        raise ValueError("Chave natural com nulos.")


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
        f"na chave {NATURAL_KEY}"
    )

    print("Chave natural OK")


def validate_ranges(df: pd.DataFrame) -> None:
    columns = [
        "taxa_alfabetizacao",
        "percentual_participacao",
        "meta_alfabetizacao",
    ]

    for column in columns:
        invalid = df[
            df[column].notna()
            & ~df[column].between(0, 100)
        ]

        assert invalid.empty, (
            f"{len(invalid)} registros inválidos "
            f"em {column}"
        )

    print("Ranges 0-100 OK")


def validate_source_reconciliation(df: pd.DataFrame) -> None:
    """Reconcilia conteúdo e cobertura com a Bronze atual, sem totais históricos."""
    try:
        from . import transform_meta_brasil_silver as source
    except ImportError:
        import transform_meta_brasil_silver as source
    import re
    expected_parts = []
    for key in source.list_parquet_files(source.BRONZE_PREFIX):
        bronze = source.read_parquet_from_s3(key)
        bronze["ano"] = source.extract_partition_value(key)
        source.validate_bronze_schema(bronze)
        bronze = source.standardize_types(bronze)
        meta_columns = [c for c in bronze.columns if re.fullmatch(r"meta_alfabetizacao_[0-9]{4}", c)]
        id_columns = [c for c in bronze.columns if c not in meta_columns]
        expected = bronze.melt(id_vars=id_columns, value_vars=meta_columns,
                               var_name="ano_meta", value_name="meta_alfabetizacao")
        expected["ano_meta"] = expected["ano_meta"].str.rsplit("_", n=1).str[-1].astype("Int64")
        expected_parts.append(expected.rename(columns={"ano": "safra"}))
    if not expected_parts:
        raise FileNotFoundError("Nenhuma partição Bronze encontrada para reconciliação.")
    expected = pd.concat(expected_parts, ignore_index=True)
    actual = df[expected.columns].sort_values(NATURAL_KEY).reset_index(drop=True)
    expected = expected.sort_values(NATURAL_KEY).reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(actual, expected, check_dtype=False)
    except AssertionError as exc:
        raise ValueError("Silver diverge da normalização da Bronze atual.") from exc
    print(f"Reconciliação Bronze x Silver OK: {len(df)} registros")


def main():
    print("=" * 60)
    print("Validando Silver: meta_alfabetizacao_brasil")
    print("=" * 60)

    df = read_silver()

    print(
        f"\nRegistros Silver: {len(df)}"
    )

    print("\nExecutando validações...")

    report_total_volume(df)
    report_partition_volume(df)
    validate_schema(df)
    validate_ano_meta(df)
    validate_ranges(df)
    validate_natural_key(df)
    validate_source_reconciliation(df)

    print(
        "\nValidação Silver concluída com sucesso."
    )


if __name__ == "__main__":
    main()