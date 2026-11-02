UF_CODE_TO_SIGLA = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}


def get_sigla_uf_from_id_municipio(
    id_municipio: str,
) -> str | None:
    """
    Retorna a sigla da UF a partir do código
    de município do IBGE.

    Os dois primeiros dígitos identificam a UF.
    """

    if id_municipio is None:
        return None

    value = str(
        id_municipio
    ).strip()

    if len(value) != 7:
        return None

    codigo_uf = value[:2]

    return UF_CODE_TO_SIGLA.get(
        codigo_uf
    )