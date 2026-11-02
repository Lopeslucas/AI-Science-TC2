import json
import time
import uuid
from datetime import datetime, timezone

import boto3


STREAM_NAME = "ai-science-tc2-stream"
AWS_REGION = "us-east-1"


def build_event(
    id_aluno: str,
    id_municipio: str,
    rede: int,
    presenca: int,
    preenchimento_caderno: int,
    alfabetizado: int,
    proficiencia: float | None,
    peso_aluno: float | None,
) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now(
            timezone.utc
        ).isoformat(),
        "ano": datetime.now(
            timezone.utc
        ).year,
        "id_municipio": id_municipio,
        "id_aluno": id_aluno,
        "rede": rede,
        "presenca": presenca,
        "preenchimento_caderno": preenchimento_caderno,
        "alfabetizado": alfabetizado,
        "proficiencia": proficiencia,
        "peso_aluno": peso_aluno,
    }


def send_event(
    client,
    event: dict,
) -> None:
    response = client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(
            event,
            ensure_ascii=False,
        ).encode("utf-8"),
        PartitionKey=event[
            "id_municipio"
        ],
    )

    print(
        "Evento enviado:",
        event["event_id"],
        "| aluno:",
        event["id_aluno"],
        "| município:",
        event["id_municipio"],
        "| shard:",
        response["ShardId"],
        "| sequence:",
        response["SequenceNumber"],
    )


def main():
    client = boto3.client(
        "kinesis",
        region_name=AWS_REGION,
    )

    events = [
        build_event(
            id_aluno="stream_001",
            id_municipio="3550308",
            rede=3,
            presenca=1,
            preenchimento_caderno=1,
            alfabetizado=1,
            proficiencia=765.42,
            peso_aluno=1.12,
        ),
        build_event(
            id_aluno="stream_002",
            id_municipio="3304557",
            rede=2,
            presenca=1,
            preenchimento_caderno=1,
            alfabetizado=0,
            proficiencia=690.15,
            peso_aluno=0.98,
        ),
        build_event(
            id_aluno="stream_003",
            id_municipio="2927408",
            rede=3,
            presenca=0,
            preenchimento_caderno=0,
            alfabetizado=0,
            proficiencia=None,
            peso_aluno=None,
        ),
    ]

    print(
        f"Enviando {len(events)} eventos..."
    )

    for event in events:
        send_event(
            client=client,
            event=event,
        )

        time.sleep(1)

    print(
        "\nEnvio concluído."
    )


if __name__ == "__main__":
    main()