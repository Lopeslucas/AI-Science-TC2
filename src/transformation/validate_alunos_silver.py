import os
import tempfile

import boto3
import pyarrow.parquet as pq


AWS_BUCKET = "ai-science-tc2"

BRONZE_PREFIX = "bronze/alunos/"
SILVER_PREFIX = "silver/alunos/"


EXPECTED_SILVER_COLUMNS = {
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
    "aluno_valido_avaliacao",
}


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


def extract_ano(key: str) -> int:
    return int(
        key.split("ano=")[1].split("/")[0]
    )


def get_parquet_metadata(
    key: str,
):
    s3 = boto3.client("s3")

    with tempfile.NamedTemporaryFile(
        suffix=".parquet",
        delete=False,
    ) as temp_file:
        temp_path = temp_file.name

    try:
        s3.download_file(
            AWS_BUCKET,
            key,
            temp_path,
        )

        parquet_file = pq.ParquetFile(
            temp_path
        )

        row_count = (
            parquet_file.metadata.num_rows
        )

        columns = set(
            parquet_file.schema_arrow.names
        )

        return row_count, columns

    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)


def get_layer_metadata(
    prefix: str,
) -> dict:
    files = list_parquet_files(
        prefix
    )

    if not files:
        raise FileNotFoundError(
            f"Nenhum Parquet encontrado em "
            f"s3://{AWS_BUCKET}/{prefix}"
        )

    result = {}

    for key in files:
        ano = extract_ano(key)

        row_count, columns = (
            get_parquet_metadata(key)
        )

        if ano in result:
            if result[ano]["columns"] != columns:
                raise ValueError(f"Schemas divergentes entre arquivos de ano={ano}.")
            result[ano]["rows"] += row_count
        else:
            result[ano] = {"rows": row_count, "columns": columns}

    return result


def validate_counts(bronze: dict, silver: dict) -> None:
    if set(bronze) != set(silver):
        raise ValueError(f"Partições divergentes: Bronze={sorted(bronze)}; Silver={sorted(silver)}")
    for ano in sorted(bronze):
        source, target = bronze[ano]["rows"], silver[ano]["rows"]
        if source != target:
            raise ValueError(f"Volume divergente em ano={ano}: Bronze={source}; Silver={target}")
        print(f"ano={ano} | bronze={source} | silver={target}")
    print(f"Registros Silver: {sum(item['rows'] for item in silver.values())}")


def validate_silver_schema(
    silver: dict,
) -> None:

    for ano, metadata in silver.items():
        columns = metadata["columns"]

        assert (
            columns
            == EXPECTED_SILVER_COLUMNS
        ), (
            f"\nSchema Silver divergente "
            f"em ano={ano}.\n"
            f"Esperado: "
            f"{sorted(EXPECTED_SILVER_COLUMNS)}\n"
            f"Encontrado: "
            f"{sorted(columns)}"
        )

    print("Schema Silver OK")


def validate_schema_consistency(
    silver: dict,
) -> None:

    schemas = [
        metadata["columns"]
        for metadata in silver.values()
    ]

    if not schemas:
        return

    first_schema = schemas[0]

    for schema in schemas[1:]:
        assert schema == first_schema, (
            "Schemas divergentes entre "
            "as partições Silver."
        )

    print(
        "Schema consistente entre partições"
    )


def main():
    print("=" * 60)
    print("Validando Silver: alunos")
    print("=" * 60)

    print(
        "\nLendo metadata Bronze..."
    )

    bronze = get_layer_metadata(
        BRONZE_PREFIX
    )

    print(
        "Lendo metadata Silver..."
    )

    silver = get_layer_metadata(
        SILVER_PREFIX
    )

    validate_counts(
        bronze=bronze,
        silver=silver,
    )

    print(
        "\nValidando schema..."
    )

    validate_silver_schema(
        silver
    )

    validate_schema_consistency(
        silver
    )

    print(
        "\nValidação Silver concluída "
        "com sucesso."
    )


if __name__ == "__main__":
    main()