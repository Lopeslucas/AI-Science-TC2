import base64
import json
from datetime import datetime, timezone

import boto3

from src.streaming.validation import validate_event

BUCKET_NAME = "ai-science-tc2"

VALID_PREFIX = "streaming/bronze/"
INVALID_PREFIX = "quarantine/streaming/"


s3 = boto3.client("s3")


def save_event(
    event: dict,
    prefix: str,
) -> None:
    now = datetime.now(
        timezone.utc
    )

    event_id = event.get(
        "event_id",
        "unknown",
    )

    key = (
        f"{prefix}"
        f"ano={now.year}/"
        f"mes={now.month:02d}/"
        f"dia={now.day:02d}/"
        f"{event_id}.json"
    )

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=key,
        Body=json.dumps(
            event,
            ensure_ascii=False,
        ).encode("utf-8"),
        ContentType="application/json",
    )

    print(
        f"Evento gravado em "
        f"s3://{BUCKET_NAME}/{key}"
    )


def lambda_handler(
    event,
    context,
):
    total = 0
    validos = 0
    invalidos = 0

    for record in event.get(
        "Records",
        [],
    ):
        total += 1

        try:
            payload = base64.b64decode(
                record["kinesis"]["data"]
            ).decode("utf-8")

            data = json.loads(
                payload
            )

            is_valid, errors = validate_event(
                data
            )

            if is_valid:
                validos += 1

                save_event(
                    event=data,
                    prefix=VALID_PREFIX,
                )

            else:
                invalidos += 1

                invalid_event = {
                    "event_id": data.get("event_id"),
                    "original_event": data,
                    "validation_errors": errors,
                    "processed_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                }

                    
                save_event(
                    event=invalid_event,
                    prefix=INVALID_PREFIX,
                )

                print(
                    "Evento inválido:",
                    errors,
                )

        except Exception as exc:
            invalidos += 1

            print(
                "Erro ao processar registro:",
                str(exc),
            )

    print(
        json.dumps(
            {
                "total_recebidos": total,
                "validos": validos,
                "invalidos": invalidos,
            }
        )
    )

    return {
        "statusCode": 200,
        "total_recebidos": total,
        "validos": validos,
        "invalidos": invalidos,
    }