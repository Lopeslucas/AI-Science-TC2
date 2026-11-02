from datetime import datetime


VALID_REDES = {1, 2, 3, 4, 5, 6}
VALID_BINARY = {0, 1}


def validate_event(event: dict) -> tuple[bool, list[str]]:
    errors = []

    required_fields = [
        "event_id",
        "event_timestamp",
        "ano",
        "id_municipio",
        "id_aluno",
        "rede",
        "presenca",
        "preenchimento_caderno",
        "alfabetizado",
    ]

    for field in required_fields:
        if field not in event:
            errors.append(
                f"Campo obrigatório ausente: {field}"
            )

    if errors:
        return False, errors

    # event_id
    if not str(event["event_id"]).strip():
        errors.append(
            "event_id vazio"
        )

    # timestamp
    try:
        datetime.fromisoformat(
            str(event["event_timestamp"]).replace(
                "Z",
                "+00:00",
            )
        )
    except ValueError:
        errors.append(
            "event_timestamp inválido"
        )

    # ano
    try:
        ano = int(
            event["ano"]
        )

        if ano < 2000:
            errors.append(
                "ano inválido"
            )

    except (TypeError, ValueError):
        errors.append(
            "ano deve ser inteiro"
        )

    # município IBGE
    id_municipio = str(
        event["id_municipio"]
    ).strip()

    if (
        len(id_municipio) != 7
        or not id_municipio.isdigit()
    ):
        errors.append(
            "id_municipio deve possuir 7 dígitos"
        )

    # rede
    try:
        rede = int(
            event["rede"]
        )

        if rede not in VALID_REDES:
            errors.append(
                "rede fora do domínio permitido"
            )

    except (TypeError, ValueError):
        errors.append(
            "rede inválida"
        )

    # campos binários
    for field in [
        "presenca",
        "preenchimento_caderno",
        "alfabetizado",
    ]:
        try:
            value = int(
                event[field]
            )

            if value not in VALID_BINARY:
                errors.append(
                    f"{field} deve ser 0 ou 1"
                )

        except (TypeError, ValueError):
            errors.append(
                f"{field} inválido"
            )

    # proficiência
    if (
        event.get("proficiencia")
        is not None
    ):
        try:
            float(
                event["proficiencia"]
            )
        except (TypeError, ValueError):
            errors.append(
                "proficiencia inválida"
            )

    # peso
    if (
        event.get("peso_aluno")
        is not None
    ):
        try:
            peso = float(
                event["peso_aluno"]
            )

            if peso < 0:
                errors.append(
                    "peso_aluno não pode ser negativo"
                )

        except (TypeError, ValueError):
            errors.append(
                "peso_aluno inválido"
            )

    return (
        len(errors) == 0,
        errors,
    )