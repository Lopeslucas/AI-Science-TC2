from io import BytesIO

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"

GOLD_META_PREFIX = "gold/meta_vs_resultado_municipio/"
ALUNOS_PREFIX = "silver/alunos/"

OUTPUT_PREFIX = "gold/indicador_municipio/"


# ============================================================
# S3
# ============================================================

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


# ============================================================
# LEITURA DA GOLD DE META
# ============================================================

def read_meta_gold_partition(
    ano: int,
) -> pd.DataFrame:

    key = (
        f"{GOLD_META_PREFIX}"
        f"ano={ano}/"
        f"meta_vs_resultado_municipio.parquet"
    )

    df = read_parquet_from_s3(
        key
    )

    df["ano"] = ano

    return df


# ============================================================
# LEITURA DE ALUNOS
# ============================================================

def find_alunos_partitions(
    ano: int,
) -> list[str]:
    files = list_parquet_files(
        ALUNOS_PREFIX
    )

    expected = (
        f"ano={ano}/"
    )

    matching = [
        key
        for key in files
        if expected in key
    ]

    if not matching:
        raise FileNotFoundError(
            f"Partição de alunos "
            f"não encontrada para ano={ano}"
        )

    return matching


# ============================================================
# PADRONIZAÇÃO
# ============================================================

def prepare_alunos(
    df: pd.DataFrame,
) -> pd.DataFrame:
    df = df.copy()

    df["id_municipio"] = (
        df["id_municipio"]
        .astype("string")
        .str.strip()
    )

    df["rede"] = (
        df["rede"]
        .astype("string")
        .str.strip()
    )

    binary_columns = [
        "presenca",
        "preenchimento_caderno",
        "alfabetizado",
    ]

    for column in binary_columns:
        df[column] = pd.to_numeric(
            df[column],
            errors="coerce",
        ).astype("Int64")

    df["proficiencia"] = pd.to_numeric(
        df["proficiencia"],
        errors="coerce",
    )

    df["peso_aluno"] = pd.to_numeric(
        df["peso_aluno"],
        errors="coerce",
    )

    df["aluno_valido_avaliacao"] = (
        df["aluno_valido_avaliacao"]
        .astype("boolean")
    )

    # Gold municipal trabalha com rede Municipal
    # código 3
    df = df[
        df["rede"] == "3"
    ].copy()

    return df


# ============================================================
# AGREGAÇÃO DOS ALUNOS
# ============================================================

def aggregate_alunos(
    df: pd.DataFrame,
) -> pd.DataFrame:

    print(
        f"Alunos da rede municipal: "
        f"{len(df)}"
    )

    # --------------------------------------------------------
    # Contagens gerais
    # --------------------------------------------------------

    base = (
        df.groupby(
            "id_municipio",
            dropna=False,
        )
        .agg(
            total_registros_alunos=(
                "id_aluno",
                "size",
            ),
            total_presentes=(
                "presenca",
                "sum",
            ),
            total_cadernos_preenchidos=(
                "preenchimento_caderno",
                "sum",
            ),
            total_alunos_validos=(
                "aluno_valido_avaliacao",
                "sum",
            ),
        )
        .reset_index()
    )

    # --------------------------------------------------------
    # Apenas alunos considerados válidos para avaliação
    # --------------------------------------------------------

    validos = df[
        df["aluno_valido_avaliacao"]
        == True
    ].copy()

    print(
        f"Alunos válidos para avaliação: "
        f"{len(validos)}"
    )

    avaliacao = (
        validos.groupby(
            "id_municipio",
            dropna=False,
        )
        .agg(
            total_alfabetizados=(
                "alfabetizado",
                "sum",
            ),
            proficiencia_media=(
                "proficiencia",
                "mean",
            ),
            peso_total=(
                "peso_aluno",
                "sum",
            ),
        )
        .reset_index()
    )

    # --------------------------------------------------------
    # Taxa simples sobre alunos válidos
    # --------------------------------------------------------

    avaliacao = avaliacao.merge(
        base[
            [
                "id_municipio",
                "total_alunos_validos",
            ]
        ],
        on="id_municipio",
        how="left",
    )

    avaliacao[
        "taxa_alfabetizacao_amostra"
    ] = (
        avaliacao[
            "total_alfabetizados"
        ]
        / avaliacao[
            "total_alunos_validos"
        ]
        * 100
    )

    # --------------------------------------------------------
    # Taxa ponderada usando peso_aluno
    # --------------------------------------------------------

    validos_peso = validos[
        validos["peso_aluno"].notna()
    ].copy()

    validos_peso[
        "alfabetizado_ponderado"
    ] = (
        validos_peso["alfabetizado"]
        * validos_peso["peso_aluno"]
    )

    weighted = (
        validos_peso.groupby(
            "id_municipio",
            dropna=False,
        )
        .agg(
            soma_alfabetizado_ponderado=(
                "alfabetizado_ponderado",
                "sum",
            ),
            soma_peso=(
                "peso_aluno",
                "sum",
            ),
        )
        .reset_index()
    )

    weighted[
        "taxa_alfabetizacao_ponderada"
    ] = (
        weighted[
            "soma_alfabetizado_ponderado"
        ]
        / weighted["soma_peso"].where(weighted["soma_peso"].gt(0))
        * 100
    )

    weighted = weighted[
        [
            "id_municipio",
            "taxa_alfabetizacao_ponderada",
        ]
    ]

    # --------------------------------------------------------
    # Consolidar indicadores
    # --------------------------------------------------------

    indicadores = base.merge(
        avaliacao[
            [
                "id_municipio",
                "total_alfabetizados",
                "proficiencia_media",
                "peso_total",
                "taxa_alfabetizacao_amostra",
            ]
        ],
        on="id_municipio",
        how="left",
    )

    indicadores = indicadores.merge(
        weighted,
        on="id_municipio",
        how="left",
    )

    return indicadores


# ============================================================
# BUILD GOLD
# ============================================================

def build_gold(
    meta: pd.DataFrame,
    indicadores: pd.DataFrame,
) -> pd.DataFrame:

    meta = meta.copy()

    meta["id_municipio"] = (
        meta["id_municipio"]
        .astype("string")
        .str.strip()
    )

    gold = meta.merge(
        indicadores,
        on="id_municipio",
        how="left",
        validate="one_to_one",
    )

    # Percentual de presença
    gold[
        "percentual_presenca"
    ] = (
        gold["total_presentes"]
        / gold["total_registros_alunos"].where(gold["total_registros_alunos"].gt(0))
        * 100
    )

    # Percentual de alunos válidos
    gold[
        "percentual_alunos_validos"
    ] = (
        gold["total_alunos_validos"]
        / gold["total_registros_alunos"].where(gold["total_registros_alunos"].gt(0))
        * 100
    )

    return gold


# ============================================================
# VALIDAÇÃO
# ============================================================

def validate_gold(
    df: pd.DataFrame,
    meta: pd.DataFrame,
) -> None:

    print(
        "\nExecutando validações Gold..."
    )

    # Reutiliza as invariantes da Gold base; indicadores não podem alterar a comparação.
    try:
        from .build_meta_vs_resultado_municipio import validate_gold as validate_meta
    except ImportError:
        from build_meta_vs_resultado_municipio import validate_gold as validate_meta
    validate_meta(df, meta)

    print(
        f"Volume Gold: "
        f"{len(df)} registros"
    )

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
        f"{len(duplicated)} municípios duplicados."
    )

    print(
        "Chave ano + id_municipio OK"
    )

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
        "\nCampos obrigatórios "
        "com valores nulos:\n"
        f"{invalid}"
    )

    print(
        "Campos obrigatórios OK"
    )

    # Taxas derivadas precisam estar entre 0 e 100
    rate_columns = [
        "taxa_alfabetizacao_amostra",
        "taxa_alfabetizacao_ponderada",
        "percentual_presenca",
        "percentual_alunos_validos",
    ]

    for column in rate_columns:
        invalid_rates = df[
            df[column].notna()
            & (
                (df[column] < 0)
                | (df[column] > 100)
            )
        ]

        assert invalid_rates.empty, (
            f"Valores inválidos em "
            f"{column}: "
            f"{len(invalid_rates)} registros"
        )

    print(
        "Faixas dos indicadores OK"
    )

    print(
        "\nResumo de cobertura dos alunos:"
    )

    print(
        f"Municípios Gold: "
        f"{len(df)}"
    )

    print(
        f"Municípios com alunos válidos: "
        f"{df['total_alunos_validos'].gt(0).sum()}"
    )


# ============================================================
# ESCRITA
# ============================================================

def write_gold(
    df: pd.DataFrame,
    ano: int,
) -> None:

    s3 = boto3.client("s3")

    buffer = BytesIO()

    # ano fica representado pela partição Hive
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
        f"{OUTPUT_PREFIX}"
        f"ano={ano}/"
        f"indicador_municipio.parquet"
    )

    s3.put_object(
        Bucket=AWS_BUCKET,
        Key=key,
        Body=buffer.getvalue(),
    )

    print(
        f"\nGold gravada: "
        f"s3://{AWS_BUCKET}/{key} "
        f"({len(df)} registros)"
    )


# ============================================================
# PROCESSAMENTO POR ANO
# ============================================================

def process_year(
    ano: int,
) -> None:

    print("=" * 60)
    print(
        f"Construindo indicador_municipio "
        f"| ano={ano}"
    )
    print("=" * 60)

    # --------------------------------------------------------
    # Gold meta x resultado
    # --------------------------------------------------------

    print(
        "\nLendo Gold "
        "meta_vs_resultado_municipio..."
    )

    meta = read_meta_gold_partition(
        ano
    )

    print(
        f"Municípios na Gold base: "
        f"{len(meta)}"
    )

    # --------------------------------------------------------
    # Silver alunos
    # --------------------------------------------------------

    alunos_keys = find_alunos_partitions(
        ano
    )

    print(
        "\nLendo Silver alunos..."
    )

    alunos = pd.concat(
        [read_parquet_from_s3(key) for key in alunos_keys],
        ignore_index=True,
    )

    alunos["ano"] = ano

    print(
        f"Alunos lidos: "
        f"{len(alunos)}"
    )

    # --------------------------------------------------------
    # Preparar alunos
    # --------------------------------------------------------

    alunos = prepare_alunos(
        alunos
    )

    # --------------------------------------------------------
    # Agregar
    # --------------------------------------------------------

    print(
        "\nCalculando indicadores "
        "por município..."
    )

    indicadores = aggregate_alunos(
        alunos
    )

    print(
        f"Municípios encontrados "
        f"nos alunos: "
        f"{len(indicadores)}"
    )

    # libera dataframe pesado
    del alunos

    # --------------------------------------------------------
    # Construir Gold
    # --------------------------------------------------------

    print(
        "\nEnriquecendo Gold..."
    )

    gold = build_gold(
        meta=meta,
        indicadores=indicadores,
    )

    # --------------------------------------------------------
    # Validar
    # --------------------------------------------------------

    validate_gold(
        gold, meta
    )

    # --------------------------------------------------------
    # Gravar
    # --------------------------------------------------------

    write_gold(
        df=gold,
        ano=ano,
    )

    print(
        f"\nAno {ano} concluído."
    )


# ============================================================
# MAIN
# ============================================================

def main():

    print(
        "Localizando partições da Gold "
        "meta_vs_resultado_municipio..."
    )

    files = list_parquet_files(
        GOLD_META_PREFIX
    )

    if not files:
        raise FileNotFoundError(
            "Nenhuma partição encontrada em "
            f"s3://{AWS_BUCKET}/"
            f"{GOLD_META_PREFIX}"
        )

    anos = sorted(
        {
            extract_ano(key)
            for key in files
        }
    )

    print(
        f"Anos disponíveis: {anos}"
    )

    for ano in anos:
        process_year(
            ano
        )

    print(
        "\nGold indicador_municipio "
        "concluída."
    )


if __name__ == "__main__":
    main()