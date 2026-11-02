from io import BytesIO

import boto3
import pandas as pd


AWS_BUCKET = "ai-science-tc2"

GOLD_PREFIX = "gold/meta_vs_resultado_uf/"


EXPECTED_COLUMNS = {
    "ano",
    "sigla_uf",
    "serie",
    "rede",
    "taxa_alfabetizacao",
    "safra_meta",
    "ano_meta",
    "meta_alfabetizacao",
    "meta_disponivel",
    "gap_meta",
    "atingiu_meta",
    "percentual_participacao",
}


NATURAL_KEY = [
    "ano",
    "sigla_uf",
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


def extract_ano(key: str) -> int:
    return int(
        key.split("ano=")[1].split("/")[0]
    )


def read_gold() -> pd.DataFrame:
    files = list_parquet_files(
        GOLD_PREFIX
    )

    if not files:
        raise FileNotFoundError(
            f"Nenhum arquivo encontrado em "
            f"s3://{AWS_BUCKET}/{GOLD_PREFIX}"
        )

    dataframes = []

    for key in files:
        df = read_parquet_from_s3(key)

        df["ano"] = extract_ano(key)

        dataframes.append(df)

    return pd.concat(
        dataframes,
        ignore_index=True,
    )


def report_observability(df: pd.DataFrame) -> None:
    print(f"Registros Gold: {len(df)}")
    print(f"UFs presentes: {df.sigla_uf.nunique()}")
    print("Volume por ano:", df.groupby("ano").size().to_dict())
    print("Distribuição atingiu_meta:", df["atingiu_meta"].value_counts(dropna=False).to_dict())
    print(f"Sem meta: {int(df['meta_alfabetizacao'].isna().sum())}")


def validate_schema(df: pd.DataFrame) -> None:
    if set(df.columns) != EXPECTED_COLUMNS:
        raise ValueError(f"Schema divergente: {sorted(df.columns)}")


def validate_required_fields(df: pd.DataFrame) -> None:
    required = NATURAL_KEY + ["taxa_alfabetizacao", "meta_disponivel"]
    if df[required].isna().any().any():
        raise ValueError("Campos obrigatórios com nulos.")
    if not pd.api.types.is_bool_dtype(df["atingiu_meta"].dtype):
        raise ValueError("atingiu_meta deve ter tipo booleano anulável.")
    if not pd.api.types.is_bool_dtype(df["meta_disponivel"].dtype):
        raise ValueError("meta_disponivel deve ter tipo booleano.")
    valid_ufs = {"AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"}
    if not df["sigla_uf"].isin(valid_ufs).all():
        raise ValueError("Sigla de UF inválida.")
    if not df["rede"].astype("string").eq("5").all():
        raise ValueError("Rede diferente da pública (5).")
    for column in ["taxa_alfabetizacao", "meta_alfabetizacao", "percentual_participacao"]:
        if not df[column].dropna().between(0, 100).all():
            raise ValueError(f"Valores fora de [0, 100]: {column}")
    available = df["meta_alfabetizacao"].notna()
    if not df["meta_disponivel"].eq(available).fillna(False).all():
        raise ValueError("meta_disponivel inconsistente.")


def validate_year(df: pd.DataFrame) -> None:
    # Sem ano fixo: valida integridade e relação temporal da versão escolhida.
    for column in ["ano", "ano_meta", "safra_meta"]:
        values = pd.to_numeric(df[column], errors="raise").dropna()
        if not (values.gt(0) & values.mod(1).eq(0)).all():
            raise ValueError(f"Ano inválido em {column}.")
    version = df["safra_meta"].notna()
    if not version.equals(df["ano_meta"].notna()):
        raise ValueError("Versão de meta incompleta.")
    if (df["meta_disponivel"] & ~version).any():
        raise ValueError("Meta disponível sem versão.")
    if not df.loc[version, "ano"].eq(df.loc[version, "ano_meta"]).all():
        raise ValueError("Ano diferente de ano_meta.")
    if not df.loc[version, "safra_meta"].le(df.loc[version, "ano"]).all():
        raise ValueError("Uso de safra futura.")


def validate_natural_key(df: pd.DataFrame) -> None:
    if df.duplicated(NATURAL_KEY).any():
        raise ValueError("Chave natural duplicada.")


def validate_gap(df: pd.DataFrame) -> None:
    expected = df["taxa_alfabetizacao"] - df["meta_alfabetizacao"]
    if not df["gap_meta"].isna().equals(expected.isna()):
        raise ValueError("Nulidade de gap_meta inconsistente.")
    known = expected.notna()
    if not df.loc[known, "gap_meta"].sub(expected[known]).abs().le(1e-9).fillna(False).all():
        raise ValueError("Cálculo de gap_meta inconsistente.")


def validate_atingiu_meta(df: pd.DataFrame) -> None:
    available = df["meta_alfabetizacao"].notna()
    if not df.loc[~available, "atingiu_meta"].isna().all():
        raise ValueError("Sem meta, atingiu_meta deve ser nulo.")
    expected = df.loc[available, "taxa_alfabetizacao"].ge(df.loc[available, "meta_alfabetizacao"])
    if not df.loc[available, "atingiu_meta"].eq(expected).fillna(False).all():
        raise ValueError("atingiu_meta inconsistente ou ausente para meta disponível.")


def main():
    print("=" * 60)
    print(
        "Validando Gold: "
        "meta_vs_resultado_uf"
    )
    print("=" * 60)

    df = read_gold()

    print(
        f"\nRegistros Gold: {len(df)}"
    )

    print("\nExecutando validações...")

    validate_schema(df)
    validate_required_fields(df)
    validate_year(df)
    validate_natural_key(df)
    validate_gap(df)
    validate_atingiu_meta(df)
    report_observability(df)

    print(
        "\nValidação Gold concluída "
        "com sucesso."
    )


if __name__ == "__main__":
    main()