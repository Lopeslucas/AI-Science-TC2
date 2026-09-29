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
        "extract_by_partition": False,
    },
    "municipio": {
        "source": f"{DATASET}.municipio",
        "partition_column": "ano",
        "extract_by_partition": False,
    },
    "meta_alfabetizacao_brasil": {
        "source": f"{DATASET}.meta_alfabetizacao_brasil",
        "partition_column": "ano",
        "extract_by_partition": False,
    },
    "meta_alfabetizacao_uf": {
        "source": f"{DATASET}.meta_alfabetizacao_uf",
        "partition_column": "ano",
        "extract_by_partition": False,
    },
    "meta_alfabetizacao_municipio": {
        "source": f"{DATASET}.meta_alfabetizacao_municipio",
        "partition_column": "ano",
        "extract_by_partition": False,
    },
    "dicionario": {
        "source": f"{DATASET}.dicionario",
        "partition_column": None,
        "extract_by_partition": False,
    },
    "alunos": {
        "source": f"{DATASET}.alunos",
        "partition_column": "ano",
        "extract_by_partition": True,
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

            # A coluna de partição NÃO deve ficar dentro do Parquet,
            # pois ela já está representada no caminho:
            # ano=2023/, ano=2024/, etc.
            df_to_write = df_partition.drop(
                columns=[partition_column]
            )

            df_to_write.to_parquet(
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

    if config["extract_by_partition"]:
        partition_column = config["partition_column"]

        partition_values = get_partition_values(
            source_table=config["source"],
            partition_column=partition_column,
        )

        total = 0

        for partition_value in partition_values:
            print(
                f"\nExtraindo "
                f"{partition_column}={partition_value}..."
            )

            df_partition = extract_partition(
                source_table=config["source"],
                partition_column=partition_column,
                partition_value=partition_value,
            )

            total += len(df_partition)

            upload_partition_to_s3(
                df=df_partition,
                table_name=table_name,
                partition_column=partition_column,
                partition_value=partition_value,
            )

            del df_partition

        print(
            f"\nTotal ingerido: {total} registros"
        )

    else:
        df = extract_table(
            source_table=config["source"]
        )

        print(
            f"Extraídos: {len(df)} registros"
        )

        upload_dataframe_to_s3(
            df=df,
            table_name=table_name,
            partition_column=config["partition_column"],
        )

    print(f"Ingestão concluída: {table_name}")


def get_partition_values(
    source_table: str,
    partition_column: str,
) -> list:
    client = bigquery.Client(project=GCP_PROJECT_ID)

    query = f"""
    SELECT DISTINCT {partition_column}
    FROM `{source_table}`
    WHERE {partition_column} IS NOT NULL
    ORDER BY {partition_column}
    """

    df = client.query(query).to_dataframe()

    return df[partition_column].tolist()


def extract_partition(
    source_table: str,
    partition_column: str,
    partition_value,
) -> pd.DataFrame:
    client = bigquery.Client(project=GCP_PROJECT_ID)

    query = f"""
    SELECT *
    FROM `{source_table}`
    WHERE {partition_column} = @partition_value
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "partition_value",
                "INT64",
                int(partition_value),
            )
        ]
    )

    return client.query(
        query,
        job_config=job_config,
    ).to_dataframe()


def upload_partition_to_s3(
    df: pd.DataFrame,
    table_name: str,
    partition_column: str,
    partition_value,
) -> None:
    s3 = boto3.client("s3")

    buffer = BytesIO()

    # A coluna de partição já está representada no caminho S3:
    # ano=2023/, ano=2024/, etc.
    df_to_write = df.drop(
        columns=[partition_column]
    )

    df_to_write.to_parquet(
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
        f"{len(df)} registros"
    )


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