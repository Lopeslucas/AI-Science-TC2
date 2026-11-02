from io import BytesIO

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"

INDICADOR_PREFIX = "gold/indicador_municipio/"
BASE_PREFIX = "gold/meta_vs_resultado_municipio/"


def list_parquet_files(prefix: str) -> list[str]:
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
        BytesIO(response["Body"].read())
    )


def extract_ano(
    key: str,
) -> int:
    return int(
        key.split("ano=")[1]
        .split("/")[0]
    )


def read_partitioned_dataset(
    prefix: str,
) -> pd.DataFrame:
    files = list_parquet_files(
        prefix
    )

    if not files:
        raise FileNotFoundError(
            f"Nenhum Parquet encontrado em "
            f"s3://{AWS_BUCKET}/{prefix}"
        )

    dataframes = []

    for key in files:
        df = read_parquet_from_s3(
            key
        )

        df["ano"] = extract_ano(
            key
        )

        dataframes.append(
            df
        )

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def validate_volume(
    base: pd.DataFrame,
    indicador: pd.DataFrame,
) -> None:
    base_counts = (
        base.groupby("ano")
        .size()
        .to_dict()
    )

    indicador_counts = (
        indicador.groupby("ano")
        .size()
        .to_dict()
    )

    assert (
        base_counts == indicador_counts
    ), (
        "\nVolume divergente por ano.\n"
        f"Base: {base_counts}\n"
        f"Indicador: {indicador_counts}"
    )

    print(
        "Reconciliação de volume OK"
    )

    print(
        f"Base: {base_counts}"
    )

    print(
        f"Indicador: {indicador_counts}"
    )


def validate_key(
    df: pd.DataFrame,
) -> None:
    duplicated = df[
        df.duplicated(
            subset=[
                "ano",
                "id_municipio",
            ],
            keep=False,
        )
    ]

    assert duplicated.empty, (
        f"Foram encontrados "
        f"{len(duplicated)} registros duplicados."
    )

    print(
        "Chave ano + id_municipio OK"
    )


def validate_required_fields(
    df: pd.DataFrame,
) -> None:
    required = [
        "ano",
        "id_municipio",
        "taxa_alfabetizacao",
        "meta_disponivel",
    ]

    null_counts = (
        df[required]
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

    print(
        "Campos obrigatórios OK"
    )


def validate_meta_logic(
    df: pd.DataFrame,
) -> None:
    com_meta = df[
        df["meta_disponivel"] == True
    ].copy()

    sem_meta = df[
        df["meta_disponivel"] == False
    ].copy()

    assert (
        com_meta["meta_alfabetizacao"]
        .notna()
        .all()
    ), (
        "Existem registros marcados como "
        "meta disponível sem valor de meta."
    )

    assert (
        sem_meta["meta_alfabetizacao"]
        .isna()
        .all()
    ), (
        "Existem registros marcados como "
        "sem meta contendo valor de meta."
    )

    assert (
        sem_meta["gap_meta"]
        .isna()
        .all()
    ), (
        "Registros sem meta devem possuir "
        "gap_meta nulo."
    )

    assert (
        sem_meta["atingiu_meta"]
        .isna()
        .all()
    ), (
        "Registros sem meta devem possuir "
        "atingiu_meta nulo."
    )

    expected_flag = (
        com_meta["taxa_alfabetizacao"]
        >= com_meta["meta_alfabetizacao"]
    )

    assert (
        com_meta["atingiu_meta"]
        == expected_flag
    ).all(), (
        "Existem registros com "
        "atingiu_meta inconsistente."
    )

    print(
        "Regra de disponibilidade de meta OK"
    )

    print(
        f"Com meta: {len(com_meta)} | "
        f"Sem meta: {len(sem_meta)}"
    )


def validate_rates(
    df: pd.DataFrame,
) -> None:
    rate_columns = [
        "taxa_alfabetizacao",
        "taxa_alfabetizacao_amostra",
        "taxa_alfabetizacao_ponderada",
        "percentual_presenca",
        "percentual_alunos_validos",
    ]

    for column in rate_columns:
        if column not in df.columns:
            continue

        invalid = df[
            df[column].notna()
            & (
                (df[column] < 0)
                | (df[column] > 100)
            )
        ]

        assert invalid.empty, (
            f"Valores fora de 0–100 "
            f"em {column}: "
            f"{len(invalid)}"
        )

    print(
        "Faixas dos indicadores OK"
    )


def validate_student_counts(
    df: pd.DataFrame,
) -> None:
    count_columns = [
        "total_registros_alunos",
        "total_presentes",
        "total_cadernos_preenchidos",
        "total_alunos_validos",
        "total_alfabetizados",
    ]

    for column in count_columns:
        if column not in df.columns:
            continue

        invalid = df[
            df[column].notna()
            & (
                df[column] < 0
            )
        ]

        assert invalid.empty, (
            f"Contagens negativas em "
            f"{column}: {len(invalid)}"
        )

    if {
        "total_alunos_validos",
        "total_registros_alunos",
    }.issubset(df.columns):

        invalid = df[
            df["total_alunos_validos"].notna()
            & df["total_registros_alunos"].notna()
            & (
                df["total_alunos_validos"]
                > df["total_registros_alunos"]
            )
        ]

        assert invalid.empty, (
            "total_alunos_validos maior que "
            "total_registros_alunos."
        )

    if {
        "total_alfabetizados",
        "total_alunos_validos",
    }.issubset(df.columns):

        invalid = df[
            df["total_alfabetizados"].notna()
            & df["total_alunos_validos"].notna()
            & (
                df["total_alfabetizados"]
                > df["total_alunos_validos"]
            )
        ]

        assert invalid.empty, (
            "total_alfabetizados maior que "
            "total_alunos_validos."
        )

    print(
        "Consistência das contagens de alunos OK"
    )


def print_observability(
    df: pd.DataFrame,
) -> None:
    print(
        "\nResumo de observabilidade:"
    )

    for ano, df_ano in df.groupby("ano"):
        print(
            f"\nano={ano}"
        )

        print(
            f"Registros: {len(df_ano)}"
        )

        print(
            "Meta disponível:"
        )

        print(
            df_ano["meta_disponivel"]
            .value_counts(
                dropna=False
            )
        )

        print(
            "Atingiu meta:"
        )

        print(
            df_ano["atingiu_meta"]
            .value_counts(
                dropna=False
            )
        )

        if (
            "total_alunos_validos"
            in df_ano.columns
        ):
            print(
                "Municípios com alunos válidos: "
                f"{df_ano['total_alunos_validos'].notna().sum()}"
            )


def main():
    print("=" * 60)
    print(
        "Validando Gold: indicador_municipio"
    )
    print("=" * 60)

    print(
        "\nLendo Gold base..."
    )

    base = read_partitioned_dataset(
        BASE_PREFIX
    )

    print(
        "\nLendo indicador_municipio..."
    )

    indicador = read_partitioned_dataset(
        INDICADOR_PREFIX
    )

    print(
        "\nExecutando validações..."
    )

    validate_volume(
        base=base,
        indicador=indicador,
    )

    validate_key(
        indicador
    )

    validate_required_fields(
        indicador
    )

    validate_meta_logic(
        indicador
    )

    validate_rates(
        indicador
    )

    validate_student_counts(
        indicador
    )

    print_observability(
        indicador
    )

    print(
        "\nValidação Gold "
        "indicador_municipio "
        "concluída com sucesso."
    )


if __name__ == "__main__":
    main()