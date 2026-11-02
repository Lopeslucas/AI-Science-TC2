# AI-Science-TC2 — Arquitetura do Projeto

## 1. Objetivo

Construir um Data Lake com dados públicos de alfabetização, organizado em arquitetura Medalhão (**Bronze, Silver e Gold**), para consultas analíticas, indicadores e futuras aplicações de Machine Learning.

Fonte principal:

```text
basedosdados.br_inep_avaliacao_alfabetizacao
```

Infraestrutura:

```text
BigQuery → Python → S3 → Glue Data Catalog → Athena
```

Bucket:

```text
s3://ai-science-tc2/
```

Região AWS:

```text
us-east-1
```

Estrutura:

```text
s3://ai-science-tc2/
├── bronze/
├── silver/
├── gold/
├── scripts/
└── athena-results/
```

---

## 2. Entidades

O pipeline trabalha com:

```text
uf
municipio
alunos
meta_alfabetizacao_brasil
meta_alfabetizacao_uf
meta_alfabetizacao_municipio
dicionario
```

Principais volumes:

| Entidade | Total |
|---|---:|
| `uf` | 145 |
| `municipio` | 23.995 |
| `alunos` | 3.867.999 |
| `meta_alfabetizacao_brasil` | 3 |
| `meta_alfabetizacao_uf` | 81 |
| `meta_alfabetizacao_municipio` | 10.704 |
| `dicionario` | 27 |

Partições principais:

```text
uf:
2023 = 70
2024 = 75

municipio:
2023 = 11.547
2024 = 12.448

alunos:
2023 = 1.747.439
2024 = 2.120.560
```

---

## 3. Camada Bronze

A Bronze preserva os dados de origem com alterações mínimas, convertendo-os para **Parquet + Snappy** e armazenando-os no S3.

Padrão de armazenamento:

```text
bronze/<entidade>/ano=<ano>/
```

Exemplo:

```text
bronze/uf/ano=2023/uf.parquet
bronze/uf/ano=2024/uf.parquet
```

A coluna `ano` é removida do schema físico do Parquet quando já está representada pela partição Hive. Isso evita duplicidade de coluna no Glue/Athena.

O `dicionario` não possui particionamento:

```text
bronze/dicionario/dicionario.parquet
```

A entidade `alunos` é extraída por ano para reduzir consumo de memória durante o processamento.

Validações da Bronze incluem:

- volume;
- schema;
- partições;
- colunas esperadas;
- consistência entre BigQuery e S3;
- metadados Parquet.

Glue Database:

```text
ai_science_tc2_bronze
```

Padrão de crawlers:

```text
crawler-bronze-<entidade>
```

**Status: Bronze concluída e disponível no Athena.**

---

## 4. Camada Silver

A Silver padroniza tipos, valida domínios, normaliza estruturas e prepara os dados para consumo analítico.

Principais tratamentos:

- identificadores tratados como string;
- validação de faixas e domínios;
- preservação de nulos estruturais;
- controle de duplicidades;
- definição de chaves naturais;
- normalização das tabelas de metas;
- processamento de `alunos` por partição.

### Chaves principais

```text
uf:
(ano, sigla_uf, serie, rede)

municipio:
(ano, id_municipio, serie, rede)
```

`id_municipio` é validado como string de 7 dígitos.

### Normalização das metas

As colunas:

```text
meta_alfabetizacao_2024
...
meta_alfabetizacao_2030
```

foram transformadas em:

```text
safra
ano_meta
meta_alfabetizacao
```

Onde:

- `safra` = versão/publicação da meta;
- `ano_meta` = ano ao qual a meta se refere.

Volumes após normalização:

```text
meta_alfabetizacao_brasil:
3 → 21

meta_alfabetizacao_uf:
81 → 567

meta_alfabetizacao_municipio:
10.704 → 74.928
```

### Alunos

A Silver de `alunos` mantém os 3.867.999 registros e cria o campo:

```text
aluno_valido_avaliacao
```

Regra:

```text
presenca = 1
AND preenchimento_caderno = 1
AND proficiencia IS NOT NULL
```

Distribuição:

```text
2023:
True  = 1.502.809
False =   244.630

2024:
True  = 1.851.852
False =   268.708
```

Glue Database:

```text
ai_science_tc2_silver
```

**Status: Silver concluída, validada, catalogada e disponível no Athena.**

---

## 5. Mapeamento de redes

O dicionário utiliza:

| Código | Rede |
|---:|---|
| `0` | Total |
| `1` | Federal |
| `2` | Estadual |
| `3` | Municipal |
| `4` | Privada |
| `5` | Pública — Estadual e Municipal |
| `6` | Pública — Federal, Estadual e Municipal |

Esse mapeamento é utilizado nos cruzamentos da Gold.

---

## 6. Camada Gold

A Gold concentra tabelas analíticas voltadas para indicadores, comparação com metas e futuras features para Machine Learning.

Glue Database:

```text
ai_science_tc2_gold
```

### `meta_vs_resultado_municipio`

Cruzamento:

```text
silver/municipio
+
silver/meta_alfabetizacao_municipio
```

Mapeamento utilizado:

```text
meta rede = Municipal
resultado rede = 3
```

Regras:

```text
gap_meta =
taxa_alfabetizacao - meta_alfabetizacao

atingiu_meta =
taxa_alfabetizacao >= meta_alfabetizacao
```

Resultado para 2024:

| Indicador | Total |
|---|---:|
| Municípios | 5.352 |
| Atingiram a meta | 2.788 |
| Não atingiram | 2.564 |

---

### `meta_vs_resultado_uf`

Cruzamento:

```text
silver/uf
+
silver/meta_alfabetizacao_uf
```

Foi identificado que a tabela de metas de UF utiliza:

```text
rede = Pública
```

Por isso, o resultado correspondente é:

```text
rede = 5
```

Resultado para 2024:

| Indicador | Total |
|---|---:|
| UFs | 25 |
| Atingiram a meta | 11 |
| Não atingiram | 14 |

A análise de cobertura mostrou que não houve perda no join:

```text
Silver rede pública: 25 UFs
Gold:                25 UFs
Perda no pipeline:    0
```

As UFs ausentes no recorte de origem são:

```text
DF
RR
```

Portanto, a ausência é uma limitação de cobertura da fonte e não uma falha do pipeline.

---

## 7. Regras de qualidade

As principais decisões do projeto são:

- identificadores preservados como strings;
- valores nulos estruturais não são preenchidos artificialmente com zero;
- metas são mantidas em formato longo;
- `safra` e `ano_meta` possuem significados distintos;
- para comparação histórica, utiliza-se a safra mais recente disponível até o ano analisado;
- Bronze e Silver são reconciliadas por volume e schema;
- cálculos Gold são validados independentemente;
- `gap_meta` e `atingiu_meta` possuem validações próprias.

---

## 8. Catálogo e consultas

Databases do Glue:

```text
ai_science_tc2_bronze
ai_science_tc2_silver
ai_science_tc2_gold
```

Padrão dos crawlers:

```text
crawler-bronze-<entidade>
crawler-silver-<entidade>
crawler-gold-<entidade>
```

Role:

```text
LabRole
```

Athena:

```text
s3://ai-science-tc2/athena-results/
```

O Athena é utilizado para:

- validação de volume;
- consulta de partições;
- análise de cobertura;
- conferência das tabelas Gold.

---

## 9. FinOps e segurança

Decisões adotadas:

- Parquet para reduzir leitura;
- compressão Snappy;
- particionamento por ano;
- processamento de grandes volumes por partição;
- crawlers separados por entidade;
- credenciais AWS temporárias fora do código;
- dados armazenados no S3 e não versionados no Git.

Credenciais utilizadas pelo ambiente AWS Academy/VocLabs:

```text
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
AWS_SESSION_TOKEN
```

---

## 10. Status atual

```text
Bronze                               ✅
Silver                               ✅

Gold:
meta_vs_resultado_municipio          ✅
meta_vs_resultado_uf                 ✅
```

Próximas tabelas Gold previstas:

```text
indicador_municipio
indicador_uf
evolucao_temporal
features_municipio
```

Após a Gold, o projeto poderá avançar para:

```text
Gold analítica
      ↓
Indicadores
      ↓
Análises exploratórias
      ↓
Features
      ↓
Machine Learning
```

Também permanece como etapa futura a implementação do fluxo de **streaming**, complementando o pipeline batch já construído.