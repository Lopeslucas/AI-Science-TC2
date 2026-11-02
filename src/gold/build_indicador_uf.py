from io import BytesIO

import boto3
import pandas as pd

from src.utils.geography import get_sigla_uf_from_id_municipio
from src.utils.geography import get_sigla_uf_from_id_municipio

AWS_BUCKET = "ai-science-tc2"

GOLD_META_PREFIX = "gold/meta_vs_resultado_uf/"
ALUNOS_PREFIX = "silver/alunos/"

OUTPUT_PREFIX = "gold/indicador_uf/"


# ============================================================
# S3
# ============================================================

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


# ============================================================
# GOLD META x RESULTADO UF
# ============================================================

def read_meta_gold_partition(
    ano: int,
) -> pd.DataFrame:
    key = (
        f"{GOLD_META_PREFIX}"
        f"ano={ano}/"
        f"meta_vs_resultado_uf.parquet"
    )

    df = read_parquet_from_s3(
        key
    )

    df["ano"] = ano

    return df


# ============================================================
# SILVER ALUNOS
# ============================================================

def find_alunos_partition(
    ano: int,
) -> str:
    files = list_parquet_files(
        ALUNOS_PREFIX
    )

    expected = f"ano={ano}/"

    matching = [
        key
        for key in files
        if expected in key
    ]

    if not matching:
        raise FileNotFoundError(
            f"Partição Silver alunos "
            f"não encontrada para ano={ano}"
        )

    return matching[0]


# ============================================================
# PREPARAÇÃO DOS ALUNOS
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

    # --------------------------------------------------------
    # Regra pública
    # --------------------------------------------------------
    # Nos microdados:
    #
    # 2 = Estadual
    # 3 = Municipal
    #
    # A Gold UF utiliza o conceito de rede pública
    # Estadual + Municipal, equivalente ao código 5
    # das tabelas agregadas.
    # --------------------------------------------------------

    df = df[
        df["rede"].isin(
            ["2", "3"]
        )
    ].copy()

    # --------------------------------------------------------
    # Município -> UF
    # --------------------------------------------------------

    df["sigla_uf"] = (
        df["id_municipio"]
        .map(
            get_sigla_uf_from_id_municipio
        )
        .astype("string")
    )

    return df


# ============================================================
# VALIDAÇÃO DO MAPEAMENTO GEOGRÁFICO
# ============================================================

def validate_geography(
    df: pd.DataFrame,
) -> None:
    invalid = df[
        df["sigla_uf"].isna()
    ]

    if not invalid.empty:
        invalid_ids = (
            invalid["id_municipio"]
            .dropna()
            .unique()
        )

        raise ValueError(
            "Existem municípios sem mapeamento "
            "para UF.\n"
            f"Quantidade de registros: "
            f"{len(invalid)}\n"
            f"IDs distintos sem mapeamento: "
            f"{len(invalid_ids)}"
        )

    print(
        "Mapeamento id_municipio -> sigla_uf OK"
    )


# ============================================================
# AGREGAÇÃO DOS MICRODADOS
# ============================================================

def aggregate_alunos(
    df: pd.DataFrame,
) -> pd.DataFrame:
    print(
        f"Alunos das redes estadual + municipal: "
        f"{len(df)}"
    )

    # --------------------------------------------------------
    # Contagens gerais por UF
    # --------------------------------------------------------

    base = (
        df.groupby(
            "sigla_uf",
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
    # Somente alunos válidos
    # --------------------------------------------------------

    validos = df[
        df["aluno_valido_avaliacao"]
        == True
    ].copy()

    print(
        f"Alunos públicos válidos para avaliação: "
        f"{len(validos)}"
    )

    avaliacao = (
        validos.groupby(
            "sigla_uf",
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
    # Taxa simples
    # --------------------------------------------------------

    avaliacao = avaliacao.merge(
        base[
            [
                "sigla_uf",
                "total_alunos_validos",
            ]
        ],
        on="sigla_uf",
        how="left",
        validate="one_to_one",
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
    # Taxa ponderada
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
            "sigla_uf",
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
        / weighted[
            "soma_peso"
        ]
        * 100
    )

    weighted = weighted[
        [
            "sigla_uf",
            "taxa_alfabetizacao_ponderada",
        ]
    ]

    # --------------------------------------------------------
    # Consolidar
    # --------------------------------------------------------

    indicadores = base.merge(
        avaliacao[
            [
                "sigla_uf",
                "total_alfabetizados",
                "proficiencia_media",
                "peso_total",
                "taxa_alfabetizacao_amostra",
            ]
        ],
        on="sigla_uf",
        how="left",
        validate="one_to_one",
    )

    indicadores = indicadores.merge(
        weighted,
        on="sigla_uf",
        how="left",
        validate="one_to_one",
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

    meta["sigla_uf"] = (
        meta["sigla_uf"]
        .astype("string")
        .str.strip()
        .str.upper()
    )

    gold = meta.merge(
        indicadores,
        on="sigla_uf",
        how="left",
        validate="one_to_one",
    )

    gold[
        "percentual_presenca"
    ] = (
        gold["total_presentes"]
        / gold["total_registros_alunos"]
        * 100
    )

    gold[
        "percentual_alunos_validos"
    ] = (
        gold["total_alunos_validos"]
        / gold["total_registros_alunos"]
        * 100
    )

    return gold


# ============================================================
# VALIDAÇÕES
# ============================================================

def validate_volume(
    meta: pd.DataFrame,
    gold: pd.DataFrame,
) -> None:
    if len(meta) != len(gold):
        raise ValueError(
            "Volume divergente após enriquecimento.\n"
            f"Entrada: {len(meta)}\n"
            f"Gold: {len(gold)}"
        )

    print(
        f"Registros de entrada: {len(meta)} | "
        f"Gold: {len(gold)}"
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
            f"Registros duplicados: "
            f"{len(duplicated)}"
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

    missing_columns = [
        column
        for column in required
        if column not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            "Colunas obrigatórias ausentes: "
            f"{missing_columns}"
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
            "Campos obrigatórios com nulos:\n"
            f"{invalid}"
        )

    print(
        "Campos obrigatórios OK"
    )


def validate_meta_logic(
    df: pd.DataFrame,
) -> None:
    com_meta = df[
        df["meta_disponivel"]
        == True
    ].copy()

    sem_meta = df[
        df["meta_disponivel"]
        == False
    ].copy()

    if not (
        com_meta[
            "meta_alfabetizacao"
        ]
        .notna()
        .all()
    ):
        raise ValueError(
            "Existem registros marcados como "
            "meta disponível sem valor da meta."
        )

    if not (
        sem_meta[
            "meta_alfabetizacao"
        ]
        .isna()
        .all()
    ):
        raise ValueError(
            "Existem registros marcados como "
            "sem meta contendo valor de meta."
        )

    if not (
        sem_meta[
            "gap_meta"
        ]
        .isna()
        .all()
    ):
        raise ValueError(
            "Registros sem meta devem possuir "
            "gap_meta nulo."
        )

    if not (
        sem_meta[
            "atingiu_meta"
        ]
        .isna()
        .all()
    ):
        raise ValueError(
            "Registros sem meta devem possuir "
            "atingiu_meta nulo."
        )

    if not com_meta.empty:
        expected = (
            com_meta[
                "taxa_alfabetizacao"
            ]
            >= com_meta[
                "meta_alfabetizacao"
            ]
        )

        if not (
            com_meta[
                "atingiu_meta"
            ]
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
                f"Valores fora da faixa 0–100 "
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

        if not invalid.empty:
            raise ValueError(
                f"Contagens negativas em "
                f"{column}: "
                f"{len(invalid)}"
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

    print(
        f"UFs na Gold: "
        f"{df['sigla_uf'].nunique()}"
    )

    if "total_alunos_validos" in df.columns:
        print(
            "UFs com alunos válidos: "
            f"{df['total_alunos_validos'].notna().sum()}"
        )

    print(
        "\nMeta disponível:"
    )

    print(
        df["meta_disponivel"]
        .value_counts(
            dropna=False
        )
    )

    print(
        "\nAtingiu meta:"
    )

    print(
        df["atingiu_meta"]
        .value_counts(
            dropna=False
        )
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
        f"indicador_uf.parquet"
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
# PROCESSAMENTO
# ============================================================

def process_year(
    ano: int,
) -> None:
    print("=" * 60)
    print(
        f"Construindo indicador_uf "
        f"| ano={ano}"
    )
    print("=" * 60)

    print(
        "\nLendo Gold "
        "meta_vs_resultado_uf..."
    )

    meta = read_meta_gold_partition(
        ano
    )

    print(
        f"UFs na Gold base: "
        f"{len(meta)}"
    )

    alunos_key = find_alunos_partition(
        ano
    )

    print(
        "\nLendo Silver alunos..."
    )

    alunos = read_parquet_from_s3(
        alunos_key
    )

    alunos["ano"] = ano

    print(
        f"Alunos lidos: "
        f"{len(alunos)}"
    )

    print(
        "\nPreparando alunos..."
    )

    alunos = prepare_alunos(
        alunos
    )

    validate_geography(
        alunos
    )

    print(
        "\nCalculando indicadores por UF..."
    )

    indicadores = aggregate_alunos(
        alunos
    )

    print(
        f"UFs encontradas nos microdados: "
        f"{len(indicadores)}"
    )

    del alunos

    print(
        "\nEnriquecendo Gold..."
    )

    gold = build_gold(
        meta=meta,
        indicadores=indicadores,
    )

    print(
        "\nExecutando validações Gold..."
    )

    validate_volume(
        meta=meta,
        gold=gold,
    )

    validate_key(
        gold
    )

    validate_required_fields(
        gold
    )

    validate_meta_logic(
        gold
    )

    validate_rates(
        gold
    )

    validate_student_counts(
        gold
    )

    print_observability(
        gold
    )

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
        "meta_vs_resultado_uf..."
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
        f"Anos disponíveis: "
        f"{anos}"
    )

    for ano in anos:
        process_year(
            ano
        )

    print(
        "\nGold indicador_uf "
        "concluída."
    )


if __name__ == "__main__":
    main()