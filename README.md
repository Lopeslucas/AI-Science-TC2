# FIAP Tech Challenge — Fase 2

## Pipeline Híbrido para Análise da Alfabetização no Brasil

Projeto desenvolvido para o **Tech Challenge — Fase 2 da Pós-Tech FIAP**, cujo objetivo é construir uma pipeline de dados em nuvem capaz de integrar diferentes fontes relacionadas ao Indicador Criança Alfabetizada.

A solução será desenvolvida seguindo uma arquitetura híbrida de dados, com processamento **Batch + Streaming**, organização em **Arquitetura Medalhão (Bronze, Silver e Gold)**, mecanismos de qualidade de dados, observabilidade e práticas de FinOps.

> **Status atual:** levantamento das fontes, profiling inicial dos dados e definição da arquitetura e dos contratos das camadas.

---

# 1. Contexto do problema

A alfabetização na infância é um dos pilares do desenvolvimento educacional e social.

No contexto do **Compromisso Nacional Criança Alfabetizada**, o objetivo nacional é garantir que as crianças brasileiras estejam alfabetizadas até o final do 2º ano do Ensino Fundamental.

A Pesquisa Alfabetiza Brasil definiu **743 pontos na escala de proficiência do Saeb** como ponto de corte a partir do qual um estudante pode ser considerado alfabetizado.

Com base nesse parâmetro, o **Indicador Criança Alfabetizada** representa o percentual de estudantes que atingem esse nível de proficiência.

O desafio proposto pela FIAP consiste em construir uma pipeline capaz de integrar informações educacionais e territoriais para possibilitar análises sobre desempenho, metas e desigualdades educacionais.

---

# 2. Objetivo técnico

Construir uma pipeline de dados em nuvem que contemple:

- ingestão de dados educacionais;
- processamento Batch;
- simulação de processamento Streaming;
- armazenamento de dados brutos;
- limpeza e padronização;
- integração entre diferentes entidades;
- regras de qualidade de dados;
- camada analítica para consumo;
- observabilidade;
- práticas de FinOps;
- possibilidade de consumo por dashboards, análises estatísticas e Machine Learning.

A arquitetura deverá seguir o padrão Medalhão:

```text
Bronze
   ↓
Silver
   ↓
Gold
```

---

# 3. Arquitetura proposta

A plataforma de nuvem escolhida para o projeto é a **Google Cloud Platform (GCP)**.

A arquitetura inicialmente planejada é:

```text
                  Base dos Dados
                        │
                        │
                 BigQuery público
                        │
                   Ingestão Batch
                        │
                        ▼
               ┌─────────────────┐
               │     BRONZE      │
               │ Dados de origem │
               └────────┬────────┘
                        │
                  Transformações
                        │
                        ▼
               ┌─────────────────┐
               │     SILVER      │
               │ Dados tratados  │
               │ e integrados    │
               └────────┬────────┘
                        │
                 Regras analíticas
                        │
                        ▼
               ┌─────────────────┐
               │      GOLD       │
               │ Dados prontos   │
               │ para consumo    │
               └─────────────────┘
                        │
             ┌──────────┼──────────┐
             │          │          │
         Dashboard   Análises      ML
```

A arquitetura de Streaming será adicionada posteriormente, após a conclusão do fluxo Batch.

Possível desenho:

```text
Producer
   ↓
Pub/Sub
   ↓
Processamento
   ↓
Bronze Streaming
   ↓
Silver Streaming
   ↓
Gold Streaming
```

> A implementação de Streaming ainda não foi iniciada.

---

# 4. Fonte dos dados

A fonte principal é a plataforma **Base dos Dados**, utilizando o dataset público disponível no BigQuery:

```text
basedosdados.br_inep_avaliacao_alfabetizacao
```

Foram identificadas as seguintes tabelas principais:

| Tabela | Conteúdo |
|---|---|
| `uf` | Indicadores agregados por Unidade Federativa |
| `municipio` | Indicadores agregados por município |
| `alunos` | Resultados individuais dos estudantes |
| `meta_alfabetizacao_brasil` | Metas nacionais de alfabetização |
| `meta_alfabetizacao_uf` | Metas por Unidade Federativa |
| `meta_alfabetizacao_municipio` | Metas municipais |
| `dicionario` | Dicionário de códigos e valores categóricos |

A opção de acesso direto via BigQuery reduz a necessidade de downloads manuais de arquivos e permite uma extração reproduzível por SQL ou Python.

---

# 5. Profiling inicial dos dados

## 5.1 Volume

Até o momento foram identificados:

| Tabela | Registros |
|---|---:|
| `uf` | 145 |
| `municipio` | 23.995 |
| `alunos` | 3.867.999 |

A tabela `alunos` representa, portanto, a maior fonte em volume e exigirá maior atenção em relação a custo de leitura, particionamento e processamento.

---

# 6. Cobertura temporal

## `uf`

| Ano | Registros |
|---|---:|
| 2023 | 70 |
| 2024 | 75 |

## `municipio`

| Ano | Registros |
|---|---:|
| 2023 | 11.547 |
| 2024 | 12.448 |

## `alunos`

| Ano | Registros |
|---|---:|
| 2023 | 1.747.439 |
| 2024 | 2.120.560 |

Os indicadores educacionais disponíveis no dataset analisado possuem atualmente cobertura para **2023 e 2024**.

---

# 7. Granularidade das tabelas

## `uf`

Granularidade esperada:

```text
ano
+
sigla_uf
+
serie
+
rede
```

Não foram encontradas duplicidades para essa combinação no levantamento inicial.

Portanto, ela será tratada inicialmente como a **chave natural candidata** da tabela.

---

## `municipio`

Granularidade esperada:

```text
ano
+
id_municipio
+
serie
+
rede
```

Também não foram identificadas duplicidades para essa combinação.

---

## `alunos`

Granularidade:

```text
aluno × ano
```

Campos identificados:

```text
ano
id_municipio
id_escola
id_aluno
caderno
serie
rede
presenca
preenchimento_caderno
alfabetizado
proficiencia
peso_aluno
```

A chave natural desta tabela ainda será formalmente validada durante o desenvolvimento.

---

# 8. Dicionário de códigos

A tabela `dicionario` permitiu identificar os valores categóricos utilizados nas fontes.

## Rede de ensino

| Código | Descrição |
|---:|---|
| 0 | Total — Federal, Estadual, Municipal e Privada |
| 1 | Federal |
| 2 | Estadual |
| 3 | Municipal |
| 4 | Privada |
| 5 | Pública — Estadual e Municipal |
| 6 | Pública — Federal, Estadual e Municipal |

Um ponto importante identificado é que as tabelas de indicadores utilizam códigos:

```text
3
5
```

enquanto as tabelas de metas utilizam valores textuais:

```text
Municipal
Pública
```

Essa diferença deverá ser normalizada na camada Silver.

Exemplo:

```text
Municipal
    ↓
rede_codigo = 3
```

e:

```text
Pública
    ↓
rede_codigo = 5
```

---

# 9. Série escolar

O código:

```text
serie = 2
```

representa:

```text
2º ano do Ensino Fundamental
```

O projeto está, portanto, alinhado diretamente ao público considerado pelo Indicador Criança Alfabetizada.

---

# 10. Campos relacionados aos alunos

O dicionário confirmou:

## Alfabetizado

```text
0 = Não
1 = Sim
```

## Presença

```text
0 = Ausente
1 = Presente
```

## Preenchimento do caderno

```text
0 = Prova não preenchida
1 = Prova preenchida
```

Essa distinção será importante durante a construção da Silver.

Um estudante pode, por exemplo, apresentar:

```text
presenca = 0
preenchimento_caderno = 0
alfabetizado = 0
proficiencia = NULL
peso_aluno = NULL
```

Esse registro não deve ser automaticamente interpretado da mesma maneira que um estudante presente que realizou a avaliação e não alcançou o ponto de corte.

Por isso, será estudada a criação de um campo derivado semelhante a:

```text
aluno_valido_avaliacao
```

Sua regra final ainda será validada antes da implementação.

---

# 11. Comportamento das distribuições por nível

As tabelas `uf` e `municipio` possuem campos:

```text
proporcao_aluno_nivel_0
proporcao_aluno_nivel_1
...
proporcao_aluno_nivel_8
```

O profiling mostrou comportamento estrutural por ano:

| Ano | Preenchimento |
|---|---|
| 2023 | campos de nível não preenchidos |
| 2024 | campos preenchidos |

Portanto:

```text
NULL em 2023
```

não será inicialmente considerado erro de qualidade.

Esse é um exemplo de **nulo estrutural**, no qual a ausência do valor decorre da própria cobertura disponível na fonte.

Não será realizada substituição automática por zero.

---

# 12. Estrutura das metas

As tabelas de metas possuem formato largo.

Exemplo:

```text
ano
meta_alfabetizacao_2024
meta_alfabetizacao_2025
meta_alfabetizacao_2026
meta_alfabetizacao_2027
meta_alfabetizacao_2028
meta_alfabetizacao_2029
meta_alfabetizacao_2030
```

Foi observado que o campo:

```text
ano
```

não representa diretamente o ano da meta.

Por exemplo:

```text
ano = 2025
meta_alfabetizacao_2024 = 60
meta_alfabetizacao_2025 = 64
...
meta_alfabetizacao_2030 = 80
```

Por isso, na camada Silver será feita uma distinção entre:

```text
safra
```

e:

```text
ano_meta
```

A transformação planejada é:

```text
ORIGEM

safra | meta_2024 | meta_2025 | meta_2026 | ...

                     ↓

SILVER

safra | ano_meta | meta_alfabetizacao
```

Exemplo:

```text
2025 | 2024 | 60.0
2025 | 2025 | 64.0
2025 | 2026 | 67.0
2025 | 2027 | 71.0
2025 | 2028 | 74.0
2025 | 2029 | 77.0
2025 | 2030 | 80.0
```

Essa transformação será realizada por meio de operação equivalente a `UNPIVOT`.

---

# 13. Safras disponíveis

Foram identificadas:

## Meta Brasil

```text
2023
2024
2025
```

Rede:

```text
Pública
```

## Meta Município

```text
2023
2024
```

Rede:

```text
Municipal
```

Isso reforça a necessidade de diferenciar:

```text
safra
```

de:

```text
ano da meta
```

---

# 14. Arquitetura Medalhão

## Bronze

Objetivo:

> preservar o dado da fonte com o mínimo possível de interpretação.

Inicialmente são consideradas entidades Bronze:

```text
bronze
├── uf
├── municipio
├── alunos
├── meta_alfabetizacao_brasil
├── meta_alfabetizacao_uf
├── meta_alfabetizacao_municipio
└── dicionario
```

Princípios:

- fidelidade à origem;
- ausência de limpeza silenciosa;
- preservação de nulos;
- preservação de códigos originais;
- possibilidade de auditoria;
- histórico de ingestões.

A Bronze deverá ser tratada como **imutável / append-only** sempre que possível.

---

# 15. Contrato inicial da Silver

A Silver será responsável pela interpretação e padronização dos dados.

Estrutura inicialmente planejada:

```text
silver
├── dim_rede
├── fato_indicador_uf
├── fato_indicador_municipio
├── fato_aluno
├── fato_meta_brasil
├── fato_meta_uf
├── fato_meta_municipio
└── dim_territorio
```

A `dim_territorio` ainda precisará ser definida, pois a tabela de indicadores municipais possui `id_municipio`, mas não possui diretamente informações como:

```text
nome_municipio
sigla_uf
regiao
```

---

# 16. Decisões de tipagem

## Identificadores

Campos como:

```text
id_municipio
id_escola
id_aluno
rede_codigo
```

serão tratados prioritariamente como `STRING`.

Mesmo quando possuem apenas números, são identificadores e não medidas.

Exemplo:

```text
id_municipio = "3550308"
```

não representa uma quantidade e não deve ser utilizado em operações como:

```text
SUM
AVG
```

Essa decisão também evita problemas relacionados a zeros à esquerda.

---

# 17. Silver — fato de indicador por UF

Estrutura prevista:

```text
ano
sigla_uf
serie_codigo
rede_codigo
taxa_alfabetizacao
media_portugues
proporcao_aluno_nivel_0
...
proporcao_aluno_nivel_8
```

Chave natural candidata:

```text
ano
+
sigla_uf
+
serie_codigo
+
rede_codigo
```

---

# 18. Silver — fato de indicador municipal

Estrutura prevista:

```text
ano
id_municipio
serie_codigo
rede_codigo
taxa_alfabetizacao
media_portugues
proporcao_aluno_nivel_0
...
proporcao_aluno_nivel_8
```

Chave natural candidata:

```text
ano
+
id_municipio
+
serie_codigo
+
rede_codigo
```

---

# 19. Silver — fato de aluno

Estrutura preliminar:

```text
ano
id_municipio
id_escola
id_aluno
caderno
serie_codigo
rede_codigo
presenca
preenchimento_caderno
alfabetizado
proficiencia
peso_aluno
```

Possível campo derivado:

```text
aluno_valido_avaliacao
```

A regra definitiva será definida após validação da metodologia do indicador.

---

# 20. Gold — produtos de dados planejados

A camada Gold ainda não foi implementada.

Os produtos inicialmente considerados são:

```text
gold
├── indicador_municipio
├── indicador_uf
├── meta_vs_resultado_municipio
├── meta_vs_resultado_uf
├── evolucao_temporal
└── features_municipio
```

---

# 21. Meta versus resultado

Uma possível tabela analítica será:

```text
gold.meta_vs_resultado_municipio
```

Estrutura prevista:

```text
ano
id_municipio
rede
taxa_alfabetizacao
meta_alfabetizacao
gap_meta
atingiu_meta
```

O `gap_meta` poderá ser calculado como:

```text
taxa_alfabetizacao - meta_alfabetizacao
```

E `atingiu_meta` como:

```text
taxa_alfabetizacao >= meta_alfabetizacao
```

porém **somente quando houver meta válida**.

Valores `NULL` de meta não serão convertidos automaticamente para zero.

---

# 22. Regras iniciais de qualidade

As primeiras regras identificadas são:

1. `id_municipio` deve possuir formato válido.
2. `id_municipio` deve ser tratado como texto.
3. `ano` deve pertencer ao domínio esperado.
4. `serie` deve estar consistente com o escopo do indicador.
5. `rede` deve pertencer ao domínio definido no dicionário.
6. `taxa_alfabetizacao` deve estar entre 0 e 100.
7. campos de proporção devem possuir valores plausíveis quando presentes.
8. a chave natural de `uf` deve permanecer única.
9. a chave natural de `municipio` deve permanecer única.
10. alunos devem possuir município compatível com o domínio territorial.
11. `NULL` estrutural não deve ser interpretado como erro automaticamente.
12. aluno ausente não deve ser considerado automaticamente equivalente a aluno avaliado e não alfabetizado.
13. a quantidade de registros deve ser reconciliada entre as etapas da pipeline.
14. registros inválidos deverão ser identificados e rastreáveis.

---

# 23. Separação entre erro e ausência estrutural

Uma das decisões do projeto é distinguir:

```text
valor ausente por erro
```

de:

```text
valor ausente porque a informação não existe naquele recorte
```

Exemplo já identificado:

```text
proporcao_aluno_nivel_0 ... nivel_8
```

em 2023.

Os campos estão vazios em 2023 e preenchidos em 2024.

Portanto, uma validação como:

```text
campo não pode ser NULL
```

seria incorreta para essa variável.

As regras de qualidade deverão considerar contexto e semântica.

---

# 24. Batch

O primeiro fluxo a ser desenvolvido será Batch.

Fluxo planejado:

```text
Base dos Dados
      ↓
BigQuery
      ↓
Extração
      ↓
Bronze
      ↓
Silver
      ↓
Qualidade
      ↓
Gold
```

A ingestão programática será priorizada em relação a processos de download manual.

---

# 25. Streaming

O desafio também exige uma arquitetura híbrida Batch + Streaming.

Como os dados educacionais utilizados são predominantemente periódicos, o Streaming será construído como uma simulação de eventos, podendo representar:

- atualização de indicador;
- retificação de valor;
- nova medição;
- atualização municipal;
- atualização de meta.

A implementação será realizada em etapa posterior.

---

# 26. FinOps

O projeto deverá ser desenvolvido buscando minimizar o custo da infraestrutura.

As decisões previstas incluem:

- uso de arquitetura serverless sempre que possível;
- seleção explícita das colunas necessárias;
- evitar `SELECT *` em extrações de grande volume;
- uso de formatos colunares quando aplicável;
- particionamento;
- controle do volume processado no BigQuery;
- execução sob demanda;
- desligamento ou remoção de recursos temporários;
- documentação de estimativa de custo.

A tabela `alunos`, com aproximadamente **3,87 milhões de registros**, será especialmente importante nesse contexto.

---

# 27. Estratégia de consultas no BigQuery

Evitar:

```sql
SELECT *
FROM tabela;
```

quando não houver necessidade de todas as colunas.

Priorizar:

```sql
SELECT
    ano,
    id_municipio,
    taxa_alfabetizacao
FROM tabela
WHERE ano = 2024;
```

O objetivo é reduzir leitura desnecessária e o volume processado.

---

# 28. Git e versionamento

Os dados não deverão ser armazenados no Git.

O repositório deverá conter:

```text
código
SQL
testes
infraestrutura
documentação
notebooks
README
configurações sem credenciais
```

Arquivos de dados serão bloqueados pelo `.gitignore`.

Exemplo:

```gitignore
.env
.venv/
__pycache__/

data/
*.csv
*.csv.gz
*.parquet

*.json
```

> Arquivos JSON de configuração que precisem ser versionados deverão ser tratados como exceção específica.

Credenciais nunca deverão ser versionadas.

---

# 29. Estrutura inicial prevista do repositório

```text
fiap-tech-challenge-fase-2/
│
├── README.md
├── .gitignore
├── .env.example
├── requirements.txt
│
├── docs/
│   ├── architecture/
│   ├── data_dictionary/
│   ├── decisions/
│   └── finops/
│
├── src/
│   ├── ingestion/
│   │   ├── batch/
│   │   └── streaming/
│   │
│   ├── transformation/
│   │   ├── silver/
│   │   └── gold/
│   │
│   ├── quality/
│   └── monitoring/
│
├── sql/
│   ├── profiling/
│   ├── silver/
│   └── gold/
│
├── tests/
│
└── notebooks/
```

---

# 30. Decisões arquiteturais registradas até o momento

## ADR-001 — Base dos Dados via BigQuery

**Decisão**

Consumir as tabelas diretamente do dataset público da Base dos Dados no BigQuery.

**Motivação**

Evitar dependência de downloads manuais e tornar a extração reproduzível.

**Trade-off**

O projeto passa a depender da disponibilidade e atualização da Base dos Dados.

---

## ADR-002 — GCP como plataforma principal

**Decisão**

Utilizar Google Cloud Platform como ambiente principal da solução.

**Serviços inicialmente previstos**

```text
BigQuery
Cloud Storage
Pub/Sub
```

Outros serviços poderão ser adicionados apenas se houver justificativa técnica.

---

## ADR-003 — Separação Bronze / Silver / Gold

**Decisão**

Aplicar Arquitetura Medalhão.

```text
Bronze = fidelidade à origem

Silver = qualidade, tipagem,
         normalização e integração

Gold = produto analítico
```

---

## ADR-004 — Códigos territoriais como texto

**Decisão**

Identificadores territoriais serão tratados como `STRING`.

Exemplo:

```text
id_municipio = "1100031"
```

Isso preserva a semântica de identificador.

---

## ADR-005 — Normalização de rede apenas na Silver

**Decisão**

A Bronze preservará:

```text
rede = 3
```

quando essa for a representação da origem.

Na Silver, o código será enriquecido com sua descrição:

```text
3 → Municipal
```

---

## ADR-006 — Metas em formato longo na Silver

**Decisão**

Transformar:

```text
meta_2024
meta_2025
...
meta_2030
```

em:

```text
ano_meta
meta_alfabetizacao
```

por meio de `UNPIVOT`.

---

## ADR-007 — NULL não será convertido automaticamente para zero

**Decisão**

Preservar diferenças entre:

```text
NULL
```

e:

```text
0
```

especialmente em:

- metas;
- distribuição por nível;
- proficiência;
- peso de aluno.

---

# 31. Evidências já coletadas

Até o momento foram executadas consultas de profiling para:

- contagem de registros;
- cobertura temporal;
- domínio da variável `rede`;
- verificação de duplicidade das chaves naturais;
- consulta ao dicionário;
- análise da disponibilidade dos níveis de proficiência.

Principais achados:

```text
UF:        145 registros
Município: 23.995 registros
Alunos:    3.867.999 registros
```

Indicadores disponíveis:

```text
2023
2024
```

Distribuição de proficiência:

```text
2023 → indisponível
2024 → disponível
```

Não foram identificadas duplicidades nas chaves naturais testadas de `uf` e `municipio`.

---

# 32. Próximas etapas

## Etapa 1 — concluída parcialmente

- [x] identificar dataset
- [x] identificar tabelas principais
- [x] fazer profiling inicial
- [x] identificar códigos de rede
- [x] validar chaves naturais de UF e município

## Etapa 2 — Bronze
- [ ]criar dataset/camada Bronze
- [ ]criar ingestão Batch
- [ ]preservar metadados de ingestão
- [ ]validar volume de entrada

## Etapa 3 — Silver
- [ ] criar dim_rede
- [ ] criar fato_indicador_uf
- [ ] criar fato_indicador_municipio
- [ ] criar fato_aluno
- [ ] normalizar metas com UNPIVOT
- [ ] criar dimensão territorial
- [ ] implementar regras de qualidade

## Etapa 4 — Gold
- [ ] indicador municipal
- [ ] indicador estadual
- [ ] meta versus resultado
- [ ] evolução temporal
- [ ] dataset de features

## Etapa 5 — Streaming
- [ ] definir contrato de evento
- [ ] desenvolver producer
- [ ] configurar Pub/Sub
- [ ] criar Bronze Streaming
- [ ] criar Silver Streaming
- [ ] criar Gold Streaming

## Etapa 6 — Operação
- [ ] observabilidade
- [ ] monitoramento
- [ ] alertas
- [ ] FinOps
- [ ] estimativa de custo

## Etapa 7 — Entrega
- [ ] documentação final
- [ ] diagrama de arquitetura
- [ ] evidências
- [ ] README final
- [ ] vídeo executivo de até 5 minutos
