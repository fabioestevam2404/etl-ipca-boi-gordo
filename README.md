# ETL IPCA × Boi Gordo — Medallion Architecture com Delta Lake

> Pipeline ETL end-to-end no Databricks CE com arquitetura Bronze/Silver/Gold,
> Delta Lake e análise de correlação entre inflação e commodity agropecuária.
> Executado e validado em abril/2026.

![Databricks](https://img.shields.io/badge/Databricks-Community%20Edition-FF3621?logo=databricks&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-4.1-E25A1C?logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-00ADD8)
![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![Methodology](https://img.shields.io/badge/Methodology-Spec--Driven-22c98a)
![Unity Catalog](https://img.shields.io/badge/Unity%20Catalog-workspace-7F77DD)

---

## Resultado

**Pearson = -0.2710** entre a variação mensal do IPCA e a variação mensal do preço do Boi Gordo, no período jan/2024 → fev/2026 (25 meses de variação calculada).

A hipótese inicial era de correlação positiva — o boi gordo compõe o sub-índice de carnes, que pesa ~8-10% na cesta do IPCA. O resultado negativo fraco sugere que outros fatores dominaram a inflação no período: energia, câmbio, serviços. Ou que o repasse existe com defasagem de 1-2 meses, o que uma análise com `lag()` no boi poderia revelar.

---

## O que o pipeline faz

Conecta duas fontes com formatos completamente diferentes:

- **BCB API** — IPCA mensal (série 433), JSON, formato `dd/MM/yyyy`
- **CEPEA/USP** — preço do Boi Gordo em R$/arroba, CSV diário desde 1997, com 4 linhas de cabeçalho antes dos dados

Normaliza, junta, calcula variações mensais e computa a correlação de Pearson entre elas.

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                    │
├──────────────┬─────────────────────────┬────────────────────┤
│   BRONZE     │        SILVER           │        GOLD        │
│              │                         │                    │
│ workspace.   │  workspace.             │  workspace.        │
│ bronze.ipca  │  silver.economia        │  gold.indicadores  │
│              │  (join 26 meses)        │  variacao_ipca     │
│ workspace.   │                         │  variacao_boi      │
│ bronze.      │                         │  Pearson: -0.2710  │
│ boi_gordo    │                         │                    │
└──────────────┴─────────────────────────┴────────────────────┘
     Unity Catalog · Delta Lake · Databricks CE Serverless
```

---

## Estrutura do Projeto

```
etl_ipca_boi/
├── CLAUDE.md                    # Contexto persistente para Claude Code
├── README.md                    # Este arquivo
│
├── 00.config/
│   └── config.ipynb             # Cria schemas no Unity Catalog
│
├── 01.bronze/
│   ├── ipca_bronze_v2.ipynb     # BCB API → workspace.bronze.ipca
│   └── boi_gordo_bronze_v2.ipynb # CEPEA CSV → workspace.bronze.boi_gordo
│
├── 02.silver/
│   └── economia_silver_v2.ipynb  # Join + normalização → workspace.silver.economia
│
├── 03.gold/
│   └── insights_gold_v2.ipynb    # Variações + Pearson → workspace.gold.indicadores
│
└── docs/
    └── ADR-001..010.md
```

---

## Pré-requisitos

### 1. Conta Databricks Community Edition

Acesse community.cloud.databricks.com. O CE atual usa Serverless — não é necessário criar cluster.

### 2. Download do CSV do Boi Gordo (ADR-001)

A CEPEA não tem API pública.

1. Acesse https://www.cepea.org.br/br/indicador/boi-gordo.aspx
2. Exporte como Excel/CSV
3. Renomeie para `boi_gordo.csv`
4. No Databricks: **Catalog → workspace → bronze → Volumes → arquivos → Upload to this volume**

### 3. Importar notebooks

Importe cada `.ipynb` no Workspace do Databricks respeitando a estrutura de pastas.

---

## Como Executar

```
1. 00.config/config.ipynb
2. 01.bronze/ipca_bronze_v2.ipynb
3. 01.bronze/boi_gordo_bronze_v2.ipynb
4. 02.silver/economia_silver_v2.ipynb
5. 03.gold/insights_gold_v2.ipynb
```

Todos os notebooks conectam automaticamente ao Serverless. Não é necessário selecionar cluster.

---

## Schemas das Tabelas Delta

### `workspace.bronze.ipca`

| Coluna | Tipo | Origem |
|---|---|---|
| `data` | String | BCB — `dd/MM/yyyy` |
| `ipca` | Double | variação % mensal |
| `data_coleta` | Timestamp | ingestão |

### `workspace.bronze.boi_gordo`

| Coluna | Tipo | Origem |
|---|---|---|
| `periodo` | String | `yyyy-MM` agregado |
| `valor_cepea` | Double | média mensal R$/arroba |
| `data_coleta` | Timestamp | ingestão |
| `fonte` | String | `CEPEA_MANUAL_UPLOAD` |

### `workspace.silver.economia`

| Coluna | Tipo | Origem |
|---|---|---|
| `data` | Date | `yyyy-MM-01` |
| `ipca` | Double | Bronze IPCA |
| `boi_gordo` | Double | Bronze Boi Gordo |
| `data_coleta_ipca` | Timestamp | herança |
| `data_coleta_boi` | Timestamp | herança |

### `workspace.gold.indicadores`

| Coluna | Tipo | Cálculo |
|---|---|---|
| `data` | Date | herança Silver |
| `ipca` | Double | herança |
| `boi_gordo` | Double | herança |
| `variacao_ipca` | Double | `(atual - ant) / ant * 100` |
| `variacao_boi` | Double | `(atual - ant) / ant * 100` |

---

## ADRs — Decisões Arquiteturais

Dez decisões registradas, seis delas descobertas durante a execução real no CE.

### ADR-001 · CEPEA: CSV manual
CEPEA não tem API. Download manual + upload via Volumes UC.

### ADR-002 · Normalização de datas via "yyyy-MM"
IPCA chega como `dd/MM/yyyy`, CEPEA como dados diários. Join direto falha. Solução: converter ambos para string `yyyy-MM` antes do join, depois converter para `DateType`.

### ADR-003 · `saveAsTable()` sem path
UC gerencia storage. Nunca usar `.option("path", ...)`.

### ADR-004 · Correlação via `collect()`
`F.corr()` retorna escalar — exibido via `collect()[0]`, não gravado como tabela separada.

### ADR-005 · `CREATE SCHEMA` sem `LOCATION`
Unity Catalog rejeita `LOCATION 'dbfs:/...'` com `AnalysisException`. Usar `CREATE SCHEMA IF NOT EXISTS workspace.bronze` sem qualquer cláusula de location.

### ADR-006 · Serverless: sem `sparkContext` nem `dbutils`
`spark.sparkContext` lança `JVM_ATTRIBUTE_NOT_SUPPORTED` no Serverless. `dbutils.fs.ls()` e `dbutils.fs.mkdirs()` também. Alternativas: `spark.version` para versão, `os.path.exists()` para arquivos.

### ADR-007 · Catálogo `main` não existe
Descobrir o catálogo disponível via `SHOW CATALOGS` antes de criar qualquer schema. Nunca hardcodar `main`. Neste workspace: `workspace`.

### ADR-008 · CEPEA: dados diários desde 1997
O CSV tem 7145 linhas (dados diários desde 23/07/1997) com 4 linhas de cabeçalho. Filtrar com `.filter(F.col("data_raw").rlike(r"^\d"))`, selecionar apenas 2024+, agregar para média mensal.

### ADR-009 · Upload via Volumes UC
No CE com Unity Catalog, o upload de arquivos vai para Volumes, não DBFS. Criar via `CREATE VOLUME IF NOT EXISTS workspace.bronze.arquivos`. Path: `/Volumes/workspace/bronze/arquivos/`.

### ADR-010 · `try_to_date` para dados CEPEA
`to_date()` explode ao encontrar a linha de cabeçalho `"Data"`. Usar `F.expr("try_to_date(data_raw, 'dd/MM/yyyy')")` que retorna `null` para strings inválidas, depois filtrar os nulls.

---

## Metodologia: Spec-Driven Development

```
Spec → Context → Execute → Validate → Commit → Evolve
```

- `CLAUDE.md` — lido automaticamente pelo Claude Code em toda sessão
- `SPEC_*.md` — contratos de schema por camada, escritos antes do código
- `ADR-*.md` — decisões arquiteturais, incluindo as descobertas durante execução
- `TASK-*.md` — tasks atômicas com critérios de Done verificáveis

As ADRs 005 a 010 foram todas descobertas durante a execução real — não estavam no spec original. Isso é exatamente o ponto: o processo de registrar decisões conforme surgem evita que o mesmo erro aconteça numa sessão futura.

---

## Stack

| Categoria | Tecnologia |
|---|---|
| Plataforma | Databricks Community Edition (Serverless) |
| Catálogo | Unity Catalog — workspace |
| Processamento | PySpark 4.1 |
| Storage | Delta Lake (ACID) |
| Linguagem | Python 3.12 |
| APIs | BCB SGS série 433 (REST) |
| Visualização | Matplotlib (dual-axis) |
| Dev | Claude Code + MCP Filesystem |
| Versionamento | Git / GitHub |

---

## Autor

**Fabio Estevam** · [@fabioestevam2404](https://github.com/fabioestevam2404)
Data Engineer · Rio de Janeiro, Brasil

---

*Desenvolvido com Spec-Driven Development + Claude Code (Anthropic)*
