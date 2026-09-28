from google.cloud import bigquery

PROJECT_ID = "angular-harmony-509500-m1"

client = bigquery.Client(project=PROJECT_ID)

query = """
SELECT
    ano,
    sigla_uf,
    serie,
    rede,
    taxa_alfabetizacao,
    media_portugues
FROM `basedosdados.br_inep_avaliacao_alfabetizacao.uf`
LIMIT 10
"""

df = client.query(query).to_dataframe()

print(df)