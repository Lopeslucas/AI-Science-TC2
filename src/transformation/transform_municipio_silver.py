from io import BytesIO

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"

BRONZE_PREFIX = "bronze/municipio/"
SILVER_PREFIX = "silver/municipio/"


EXPECTED_COLUMNS = {
    "ano",
    "id_municipio",
    "serie",
    "rede",
    "taxa_alfabetizacao",
    "media_portugues",
    "proporcao_aluno_nivel_0",
    "proporcao_aluno_nivel_1",
    "proporcao_aluno_nivel_2",
    "proporcao_aluno_nivel_3",
    "proporcao_aluno_nivel_4",
    "proporcao_aluno_nivel_5",
    "proporcao_aluno_nivel_6",
    "proporcao_aluno_nivel_7",
    "proporcao_aluno_nivel_8",
}


VALID_REDE = {
    "0",
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
}


NATURAL_KEY = [
    "ano",
    "id_municipio",
    "serie",
    "rede",
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


def read_bronze_municipio() -> pd.DataFrame:
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

        ano = extract_partition_value(key)

        df["ano"] = ano

        dataframes.append(df)

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


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

    # Identificador: manter como string
    df["id_municipio"] = (
        df["id_municipio"]
        .astype("string")
        .str.strip()
    )

    df["serie"] = (
        df["serie"]
        .astype("string")
        .str.strip()
    )

    df["rede"] = (
        df["rede"]
        .astype("string")
        .str.strip()
    )

    numeric_columns = [
        "taxa_alfabetizacao",
        "media_portugues",
        "proporcao_aluno_nivel_0",
        "proporcao_aluno_nivel_1",
        "proporcao_aluno_nivel_2",
        "proporcao_aluno_nivel_3",
        "proporcao_aluno_nivel_4",
        "proporcao_aluno_nivel_5",
        "proporcao_aluno_nivel_6",
        "proporcao_aluno_nivel_7",
        "proporcao_aluno_nivel_8",
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    return df


def validate_schema(
    df: pd.DataFrame,
) -> None:
    columns = set(df.columns)

    assert columns == EXPECTED_COLUMNS, (
        "\nSchema inesperado.\n"
        f"Esperado: {sorted(EXPECTED_COLUMNS)}\n"
        f"Encontrado: {sorted(columns)}"
    )


def validate_required_fields(
    df: pd.DataFrame,
) -> None:
    required_columns = [
        "ano",
        "id_municipio",
        "serie",
        "rede",
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


def validate_id_municipio(
    df: pd.DataFrame,
) -> None:
    invalid = df[
        ~df["id_municipio"].str.fullmatch(
            r"\d{7}",
            na=False,
        )
    ]

    assert invalid.empty, (
        f"Foram encontrados "
        f"{len(invalid)} registros com "
        f"id_municipio inválido."
    )


def validate_rede(
    df: pd.DataFrame,
) -> None:
    invalid_values = set(
        df.loc[
            ~df["rede"].isin(VALID_REDE),
            "rede",
        ]
        .dropna()
        .unique()
    )

    assert not invalid_values, (
        f"Valores inválidos em rede: "
        f"{invalid_values}"
    )


def validate_taxa_alfabetizacao(
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
        f"Existem {len(invalid)} registros com "
        f"taxa_alfabetizacao fora de 0-100."
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


def validate_quality(
    df: pd.DataFrame,
) -> None:
    print("\nExecutando validações de qualidade...")

    validate_schema(df)
    print("Schema OK")

    validate_required_fields(df)
    print("Campos obrigatórios OK")

    validate_id_municipio(df)
    print("id_municipio OK")

    validate_rede(df)
    print("Domínio de rede OK")

    validate_taxa_alfabetizacao(df)
    print("Taxa de alfabetização OK")

    validate_natural_key(df)
    print("Chave natural OK")


def write_silver(
    df: pd.DataFrame,
) -> None:
    s3 = boto3.client("s3")

    for ano, df_ano in df.groupby("ano"):
        buffer = BytesIO()

        # "ano" fica representado pela partição Hive no S3.
        df_to_write = df_ano.drop(
            columns=["ano"]
        )

        df_to_write.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
            compression="snappy",
        )

        key = (
            f"{SILVER_PREFIX}"
            f"ano={int(ano)}/"
            f"municipio.parquet"
        )

        s3.put_object(
            Bucket=AWS_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
        )

        print(
            f"Silver gravada: "
            f"s3://{AWS_BUCKET}/{key} "
            f"({len(df_ano)} registros)"
        )


def main():
    print("Lendo Bronze Municipio...")

    df = read_bronze_municipio()

    print(
        f"Registros lidos da Bronze: "
        f"{len(df)}"
    )

    print("\nPadronizando tipos...")

    df = standardize_types(df)

    validate_quality(df)

    print("\nGravando Silver...")

    write_silver(df)

    print("\nTransformação Municipio concluída.")


if __name__ == "__main__":
    main()