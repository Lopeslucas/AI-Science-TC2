from io import BytesIO

import boto3
import pandas as pd
from google.cloud import bigquery
import argparse

GCP_PROJECT_ID = "angular-harmony-509500-m1"
AWS_BUCKET = "ai-science-tc2"

DATASET = "basedosdados.br_inep_avaliacao_alfabetizacao"

TABLES = {
    "uf": {
        "source": f"{DATASET}.uf",
        "partition_column": "ano",
    },
    "municipio": {
        "source": f"{DATASET}.municipio",
        "partition_column": "ano",
    },
    "meta_alfabetizacao_brasil": {
        "source": f"{DATASET}.meta_alfabetizacao_brasil",
        "partition_column": "ano",
    },
    "meta_alfabetizacao_uf": {
        "source": f"{DATASET}.meta_alfabetizacao_uf",
        "partition_column": "ano",
    },
    "meta_alfabetizacao_municipio": {
        "source": f"{DATASET}.meta_alfabetizacao_municipio",
        "partition_column": "ano",
    },
    "dicionario": {
        "source": f"{DATASET}.dicionario",
        "partition_column": None,
    },
}


def extract_table(source_table: str) -> pd.DataFrame:
    client = bigquery.Client(project=GCP_PROJECT_ID)

    query = f"""
    SELECT *
    FROM `{source_table}`
    """

    return client.query(query).to_dataframe()


def upload_dataframe_to_s3(
    df: pd.DataFrame,
    table_name: str,
    partition_column: str | None,
) -> None:
    s3 = boto3.client("s3")

    if partition_column:
        for partition_value, df_partition in df.groupby(partition_column):
            buffer = BytesIO()

            df_partition.to_parquet(
                buffer,
                index=False,
                engine="pyarrow",
                compression="snappy",
            )

            key = (
                f"bronze/{table_name}/"
                f"{partition_column}={partition_value}/"
                f"{table_name}.parquet"
            )

            s3.put_object(
                Bucket=AWS_BUCKET,
                Key=key,
                Body=buffer.getvalue(),
            )

            print(
                f"{table_name} | "
                f"{partition_column}={partition_value} | "
                f"{len(df_partition)} registros"
            )

    else:
        buffer = BytesIO()

        df.to_parquet(
            buffer,
            index=False,
            engine="pyarrow",
            compression="snappy",
        )

        key = f"bronze/{table_name}/{table_name}.parquet"

        s3.put_object(
            Bucket=AWS_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
        )

        print(
            f"{table_name} | "
            f"{len(df)} registros"
        )


def ingest_table(table_name: str) -> None:
    config = TABLES[table_name]

    print(f"\nIniciando ingestão: {table_name}")

    df = extract_table(
        source_table=config["source"]
    )

    print(f"Extraídos: {len(df)} registros")

    upload_dataframe_to_s3(
        df=df,
        table_name=table_name,
        partition_column=config["partition_column"],
    )

    print(f"Ingestão concluída: {table_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingestão Batch da camada Bronze"
    )

    parser.add_argument(
        "table",
        choices=TABLES.keys(),
        help="Tabela que será ingerida",
    )

    args = parser.parse_args()

    ingest_table(args.table)