from io import BytesIO

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"

MUNICIPIO_PREFIX = "silver/municipio/"
META_MUNICIPIO_PREFIX = "silver/meta_alfabetizacao_municipio/"

GOLD_PREFIX = "gold/meta_vs_resultado_municipio/"


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

    return sorted(keys)


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
    partition_column: str,
) -> int:
    return int(
        key.split(
            f"{partition_column}="
        )[1].split("/")[0]
    )


def read_silver_municipio() -> pd.DataFrame:
    files = list_parquet_files(
        MUNICIPIO_PREFIX
    )

    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em "
            f"s3://{AWS_BUCKET}/{MUNICIPIO_PREFIX}"
        )

    dataframes = []

    for key in files:
        df = read_parquet_from_s3(
            key
        )

        ano = extract_partition_value(
            key=key,
            partition_column="ano",
        )

        df["ano"] = ano

        dataframes.append(df)

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def read_silver_meta_municipio() -> pd.DataFrame:
    files = list_parquet_files(
        META_MUNICIPIO_PREFIX
    )

    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em "
            f"s3://{AWS_BUCKET}/{META_MUNICIPIO_PREFIX}"
        )

    dataframes = []

    for key in files:
        df = read_parquet_from_s3(
            key
        )

        safra = extract_partition_value(
            key=key,
            partition_column="safra",
        )

        df["safra"] = safra

        dataframes.append(df)

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def prepare_resultados(
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

    df["ano"] = pd.to_numeric(
        df["ano"],
        errors="raise",
    ).astype("Int64")

    df["taxa_alfabetizacao"] = pd.to_numeric(
        df["taxa_alfabetizacao"],
        errors="raise",
    )

    # Código municipal conforme o dicionário da fonte.
    df = df[df["rede"] == "3"].copy()

    return df


def prepare_metas(
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

    df["safra"] = pd.to_numeric(
        df["safra"],
        errors="raise",
    ).astype("Int64")

    df["ano_meta"] = pd.to_numeric(
        df["ano_meta"],
        errors="raise",
    ).astype("Int64")

    df["meta_alfabetizacao"] = pd.to_numeric(
        df["meta_alfabetizacao"],
        errors="raise",
    )

    df = df[df["rede"].str.casefold() == "municipal"].copy()

    return df


def select_latest_meta_version(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Para cada município e ano_meta, seleciona
    a safra mais recente disponível
    até o próprio ano_meta.
    """

    df = df[
        df["safra"]
        <= df["ano_meta"]
    ].copy()

    version_key = ["id_municipio", "ano_meta", "safra"]
    if df[version_key].isna().any().any():
        raise ValueError("Chaves de versão da meta não podem ser nulas.")
    if df.duplicated(version_key).any():
        raise ValueError("Metas duplicadas para município, ano_meta e safra.")

    df = df.sort_values(
        [
            "id_municipio",
            "ano_meta",
            "safra",
        ]
    )

    df = df.drop_duplicates(
        subset=[
            "id_municipio",
            "ano_meta",
        ],
        keep="last",
    )

    return df


def build_gold(
    resultados: pd.DataFrame,
    metas: pd.DataFrame,
) -> pd.DataFrame:
    metas = select_latest_meta_version(
        metas
    )

    gold = resultados.merge(
        metas[
            [
                "id_municipio",
                "safra",
                "ano_meta",
                "meta_alfabetizacao",
                "nivel_alfabetizacao",
                "percentual_participacao",
            ]
        ],
        left_on=[
            "id_municipio",
            "ano",
        ],
        right_on=[
            "id_municipio",
            "ano_meta",
        ],
        how="left",
        validate="many_to_one",
    )

    gold["gap_meta"] = (
        gold["taxa_alfabetizacao"]
        - gold["meta_alfabetizacao"]
    )

    gold["meta_disponivel"] = gold["meta_alfabetizacao"].notna().astype("boolean")
    gold["atingiu_meta"] = pd.Series(pd.NA, index=gold.index, dtype="boolean")
    comparavel = gold["meta_disponivel"] & gold["taxa_alfabetizacao"].notna()
    gold.loc[comparavel, "atingiu_meta"] = (
        gold.loc[comparavel, "taxa_alfabetizacao"]
        >= gold.loc[comparavel, "meta_alfabetizacao"]
    )

    gold = gold[
        [
            "ano",
            "id_municipio",
            "serie",
            "rede",
            "taxa_alfabetizacao",
            "safra",
            "ano_meta",
            "meta_alfabetizacao",
            "meta_disponivel",
            "gap_meta",
            "atingiu_meta",
            "nivel_alfabetizacao",
            "percentual_participacao",
        ]
    ]

    gold = gold.rename(
        columns={
            "safra": "safra_meta",
        }
    )

    return gold


def validate_gold(df: pd.DataFrame, resultados: pd.DataFrame) -> None:
    """Invariantes e reconciliação; volumes/distribuições são observabilidade."""
    key = ["ano", "id_municipio", "serie", "rede"]
    required = key + ["taxa_alfabetizacao", "meta_disponivel"]
    if df[required].isna().any().any():
        raise ValueError("Campos obrigatórios com nulos.")
    if df.duplicated(key).any():
        raise ValueError("Chave natural Gold duplicada.")
    # Compara as chaves completas, não apenas uma contagem de snapshot.
    source_keys = resultados[key].astype("string").sort_values(key).reset_index(drop=True)
    gold_keys = df[key].astype("string").sort_values(key).reset_index(drop=True)
    if not source_keys.equals(gold_keys):
        raise ValueError("Gold não preservou as chaves dos resultados de entrada.")
    if not df["id_municipio"].astype("string").str.fullmatch(r"[0-9]{7}").all():
        raise ValueError("id_municipio deve conter sete dígitos.")
    if not df["rede"].eq("3").all():
        raise ValueError("A Gold municipal deve conter a rede municipal (3).")
    for column in ["taxa_alfabetizacao", "meta_alfabetizacao", "percentual_participacao"]:
        values = df[column].dropna()
        if not values.between(0, 100).all():
            raise ValueError(f"Valores fora de [0, 100] em {column}.")
    has_version = df["safra_meta"].notna()
    if not df["ano_meta"].notna().equals(has_version):
        raise ValueError("Safra e ano_meta devem identificar a mesma versão.")
    if not df.loc[has_version, "ano"].eq(df.loc[has_version, "ano_meta"]).all():
        raise ValueError("Ano do resultado diferente de ano_meta.")
    if not df.loc[has_version, "safra_meta"].le(df.loc[has_version, "ano"]).all():
        raise ValueError("Meta de safra futura usada na comparação histórica.")
    available = df["meta_alfabetizacao"].notna()
    if not df["meta_disponivel"].eq(available).fillna(False).all():
        raise ValueError("meta_disponivel inconsistente.")
    if (available & ~has_version).any():
        raise ValueError("Meta disponível sem identificação de versão.")
    if not df.loc[~available, ["gap_meta", "atingiu_meta"]].isna().all().all():
        raise ValueError("Sem meta, gap_meta e atingiu_meta devem ser nulos.")
    expected_gap = df.loc[available, "taxa_alfabetizacao"] - df.loc[available, "meta_alfabetizacao"]
    valid_gap = df.loc[available, "gap_meta"].sub(expected_gap).abs().le(1e-9)
    if not valid_gap.fillna(False).all():
        raise ValueError("gap_meta inconsistente.")
    expected_flag = df.loc[available, "taxa_alfabetizacao"].ge(df.loc[available, "meta_alfabetizacao"])
    if not df.loc[available, "atingiu_meta"].eq(expected_flag).fillna(False).all():
        raise ValueError("atingiu_meta inconsistente.")
    print(f"Registros de entrada: {len(resultados)} | Gold: {len(df)}")
    print(f"Com meta: {int(available.sum())} | Sem meta: {int((~available).sum())}")
    print(df["atingiu_meta"].value_counts(dropna=False))


def write_gold(
    df: pd.DataFrame,
) -> None:
    s3 = boto3.client("s3")

    for ano, df_ano in df.groupby("ano"):
        buffer = BytesIO()

        # ano fica representado pelo
        # particionamento Hive no S3
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
            f"{GOLD_PREFIX}"
            f"ano={int(ano)}/"
            f"meta_vs_resultado_municipio.parquet"
        )

        s3.put_object(
            Bucket=AWS_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
        )

        print(
            f"Gold gravada: "
            f"s3://{AWS_BUCKET}/{key} "
            f"({len(df_ano)} registros)"
        )


def main():
    print(
        "Lendo Silver Município..."
    )

    resultados = read_silver_municipio()

    print(
        f"Resultados lidos: "
        f"{len(resultados)}"
    )

    print(
        "\nLendo Silver Meta Município..."
    )

    metas = read_silver_meta_municipio()

    print(
        f"Metas lidas: "
        f"{len(metas)}"
    )

    print(
        "\nPreparando resultados..."
    )

    resultados = prepare_resultados(
        resultados
    )

    print(
        f"Resultados rede municipal: "
        f"{len(resultados)}"
    )

    print(
        "\nPreparando metas..."
    )

    metas = prepare_metas(
        metas
    )

    print(
        f"Metas municipais: "
        f"{len(metas)}"
    )

    print(
        "\nConstruindo Gold..."
    )

    gold = build_gold(
        resultados=resultados,
        metas=metas,
    )

    validate_gold(
        gold, resultados
    )

    print(
        "\nGravando Gold..."
    )

    write_gold(
        gold
    )

    print(
        "\nGold meta_vs_resultado_municipio "
        "concluída."
    )


if __name__ == "__main__":
    main()