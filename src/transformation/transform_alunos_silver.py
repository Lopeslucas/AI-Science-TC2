from io import BytesIO

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"

BRONZE_PREFIX = "bronze/alunos/"
SILVER_PREFIX = "silver/alunos/"


EXPECTED_COLUMNS = {
    "ano",
    "id_municipio",
    "id_escola",
    "id_aluno",
    "caderno",
    "serie",
    "rede",
    "presenca",
    "preenchimento_caderno",
    "alfabetizado",
    "proficiencia",
    "peso_aluno",
}


VALID_BINARY_VALUES = {
    0,
    1,
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


def list_parquet_files(
    prefix: str,
) -> list[str]:
    s3 = boto3.client("s3")

    paginator = s3.get_paginator(
        "list_objects_v2"
    )

    keys = []

    for page in paginator.paginate(
        Bucket=AWS_BUCKET,
        Prefix=prefix,
    ):
        for obj in page.get(
            "Contents",
            [],
        ):
            key = obj["Key"]

            if key.endswith(".parquet"):
                keys.append(key)

    return sorted(keys)


def read_parquet_from_s3(
    key: str,
) -> pd.DataFrame:
    s3 = boto3.client("s3")

    response = s3.get_object(
        Bucket=AWS_BUCKET,
        Key=key,
    )

    return pd.read_parquet(
        BytesIO(
            response["Body"].read()
        )
    )


def extract_partition_value(
    key: str,
    partition_column: str = "ano",
) -> int:
    partition_text = key.split(
        f"{partition_column}="
    )[1].split("/")[0]

    return int(partition_text)


def validate_schema(
    df: pd.DataFrame,
) -> None:
    columns = set(df.columns)

    assert columns == EXPECTED_COLUMNS, (
        "\nSchema inesperado.\n"
        f"Esperado: {sorted(EXPECTED_COLUMNS)}\n"
        f"Encontrado: {sorted(columns)}"
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

    string_columns = [
        "id_municipio",
        "id_escola",
        "id_aluno",
        "caderno",
        "serie",
        "rede",
    ]

    for column in string_columns:
        df[column] = (
            df[column]
            .astype("string")
            .str.strip()
        )

    binary_columns = [
        "presenca",
        "preenchimento_caderno",
        "alfabetizado",
    ]

    for column in binary_columns:
        df[column] = (
            pd.to_numeric(
                df[column],
                errors="coerce",
            )
            .astype("Int64")
        )

    numeric_columns = [
        "proficiencia",
        "peso_aluno",
    ]

    for column in numeric_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        )

    return df


def add_quality_fields(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df = df.copy()

    # Regra operacional preliminar:
    # aluno considerado válido para avaliação quando:
    # - esteve presente
    # - preencheu a prova
    # - possui proficiência
    #
    # Essa coluna ajuda a evitar interpretar aluno ausente
    # como aluno efetivamente avaliado e não alfabetizado.

    df["aluno_valido_avaliacao"] = (
        (df["presenca"] == 1)
        & (df["preenchimento_caderno"] == 1)
        & (df["proficiencia"].notna())
    ).astype("boolean")

    return df


def validate_required_fields(
    df: pd.DataFrame,
) -> None:
    required_columns = [
        "ano",
        "id_municipio",
        "id_aluno",
        "serie",
        "rede",
        "presenca",
        "preenchimento_caderno",
        "alfabetizado",
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


def validate_binary_columns(
    df: pd.DataFrame,
) -> None:
    columns = [
        "presenca",
        "preenchimento_caderno",
        "alfabetizado",
    ]

    for column in columns:
        invalid_values = set(
            df.loc[
                ~df[column].isin(
                    VALID_BINARY_VALUES
                ),
                column,
            ]
            .dropna()
            .unique()
        )

        assert not invalid_values, (
            f"Valores inválidos em "
            f"{column}: {invalid_values}"
        )


def validate_rede(
    df: pd.DataFrame,
) -> None:
    invalid_values = set(
        df.loc[
            ~df["rede"].isin(
                VALID_REDE
            ),
            "rede",
        ]
        .dropna()
        .unique()
    )

    assert not invalid_values, (
        f"Valores inválidos em rede: "
        f"{invalid_values}"
    )


def validate_absence_consistency(
    df: pd.DataFrame,
) -> None:
    """
    Não tratamos alfabetizado=0 como erro quando
    presenca=0.

    A validação aqui apenas verifica uma inconsistência
    operacional mais forte:
    aluno ausente com prova preenchida.
    """

    invalid = df[
        (df["presenca"] == 0)
        & (df["preenchimento_caderno"] == 1)
    ]

    assert invalid.empty, (
        f"Foram encontrados "
        f"{len(invalid)} registros com "
        f"presenca=0 e preenchimento_caderno=1."
    )


def validate_quality(
    df: pd.DataFrame,
) -> None:
    print(
        "\nExecutando validações "
        "de qualidade..."
    )

    validate_required_fields(df)
    print("Campos obrigatórios OK")

    validate_id_municipio(df)
    print("id_municipio OK")

    validate_binary_columns(df)
    print("Domínios binários OK")

    validate_rede(df)
    print("Domínio de rede OK")

    validate_absence_consistency(df)
    print("Consistência presença/prova OK")


def write_silver_partition(
    df: pd.DataFrame,
    ano: int,
) -> None:
    s3 = boto3.client("s3")

    buffer = BytesIO()

    # "ano" fica representado pelo caminho Hive
    df_to_write = df.drop(
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
        f"ano={ano}/"
        f"alunos.parquet"
    )

    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=key,
        Body=buffer.getvalue(),
    )

    print(
        f"Silver gravada: "
        f"s3://{AWS_BUCKET}/{key} "
        f"({len(df)} registros)"
    )


def process_partition(
    key: str | list[str],
) -> None:
    keys = [key] if isinstance(key, str) else key
    years = {extract_partition_value(item) for item in keys}
    if len(years) != 1:
        raise ValueError("O processamento deve receber arquivos de uma única partição anual.")
    ano = years.pop()

    print("=" * 60)
    print(
        f"Processando alunos | ano={ano}"
    )
    print("=" * 60)

    parts = []
    for item in keys:
        part = read_parquet_from_s3(item)
        part["ano"] = ano
        validate_schema(part)
        parts.append(part)
    df = pd.concat(parts, ignore_index=True)

    # Reconstrói coluna da partição
    df["ano"] = ano

    print(
        f"Registros Bronze: "
        f"{len(df)}"
    )

    print(
        "\nValidando schema Bronze..."
    )

    validate_schema(df)

    print("Schema Bronze OK")

    print(
        "\nPadronizando tipos..."
    )

    df = standardize_types(df)

    print(
        "\nCriando campos de qualidade..."
    )

    df = add_quality_fields(df)

    validate_quality(df)

    print(
        "\nResumo aluno_valido_avaliacao:"
    )

    print(
        df["aluno_valido_avaliacao"]
        .value_counts(
            dropna=False
        )
    )

    print(
        "\nGravando Silver..."
    )

    write_silver_partition(
        df=df,
        ano=ano,
    )

    # Libera memória antes da próxima partição
    del df

    print(
        f"\nPartição ano={ano} concluída."
    )


def main():
    print(
        "Localizando partições Bronze de alunos..."
    )

    parquet_files = list_parquet_files(
        BRONZE_PREFIX
    )

    if not parquet_files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em "
            f"s3://{AWS_BUCKET}/{BRONZE_PREFIX}"
        )

    print(
        f"Partições encontradas: "
        f"{len(parquet_files)}"
    )

    partitions = {}
    for key in parquet_files:
        partitions.setdefault(extract_partition_value(key), []).append(key)
    for ano in sorted(partitions):
        process_partition(partitions[ano])

    print(
        "\nTransformação Alunos concluída."
    )


if __name__ == "__main__":
    main()