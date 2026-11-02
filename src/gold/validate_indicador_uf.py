from io import BytesIO

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"

BASE_PREFIX = "gold/meta_vs_resultado_uf/"
INDICADOR_PREFIX = "gold/indicador_uf/"


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
            f"Nenhum arquivo encontrado em "
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

    if base_counts != indicador_counts:
        raise ValueError(
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
                "sigla_uf",
            ],
            keep=False,
        )
    ]

    if not duplicated.empty:
        raise ValueError(
            "Existem registros duplicados para "
            "ano + sigla_uf.\n"
            f"Quantidade: {len(duplicated)}"
        )

    print(
        "Chave ano + sigla_uf OK"
    )


def validate_required_fields(
    df: pd.DataFrame,
) -> None:
    required = [
        "ano",
        "sigla_uf",
        "taxa_alfabetizacao",
        "meta_disponivel",
    ]

    missing = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing:
        raise ValueError(
            f"Colunas obrigatórias ausentes: "
            f"{missing}"
        )

    null_counts = (
        df[required]
        .isna()
        .sum()
    )

    invalid = null_counts[
        null_counts > 0
    ]

    if not invalid.empty:
        raise ValueError(
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

    if not (
        com_meta["meta_alfabetizacao"]
        .notna()
        .all()
    ):
        raise ValueError(
            "Registro com meta_disponivel=True "
            "sem valor de meta."
        )

    if not (
        sem_meta["meta_alfabetizacao"]
        .isna()
        .all()
    ):
        raise ValueError(
            "Registro com meta_disponivel=False "
            "contendo meta."
        )

    if not (
        sem_meta["gap_meta"]
        .isna()
        .all()
    ):
        raise ValueError(
            "Registro sem meta deve possuir "
            "gap_meta nulo."
        )

    if not (
        sem_meta["atingiu_meta"]
        .isna()
        .all()
    ):
        raise ValueError(
            "Registro sem meta deve possuir "
            "atingiu_meta nulo."
        )

    if not com_meta.empty:
        expected = (
            com_meta["taxa_alfabetizacao"]
            >= com_meta["meta_alfabetizacao"]
        )

        if not (
            com_meta["atingiu_meta"]
            == expected
        ).all():
            raise ValueError(
                "Existem valores inconsistentes "
                "em atingiu_meta."
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

        if not invalid.empty:
            raise ValueError(
                f"Valores fora de 0–100 em "
                f"{column}: {len(invalid)}"
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

        if not invalid.empty:
            raise ValueError(
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

        if not invalid.empty:
            raise ValueError(
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

        if not invalid.empty:
            raise ValueError(
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
            f"UFs: "
            f"{df_ano['sigla_uf'].nunique()}"
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
                "UFs com alunos válidos: "
                f"{df_ano['total_alunos_validos'].notna().sum()}"
            )


def main():
    print("=" * 60)
    print(
        "Validando Gold: indicador_uf"
    )
    print("=" * 60)

    print(
        "\nLendo Gold base..."
    )

    base = read_partitioned_dataset(
        BASE_PREFIX
    )

    print(
        "\nLendo indicador_uf..."
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
        "\nValidação Gold indicador_uf "
        "concluída com sucesso."
    )


if __name__ == "__main__":
    main()