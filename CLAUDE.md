# CLAUDE.md — ETL IPCA × Boi Gordo

> Arquivo lido automaticamente pelo Claude Code em toda sessão.
> Atualizado após execução real no Databricks CE — abril/2026.

---

## Stack & Ambiente

| Item | Valor |
|---|---|
| Plataforma | Databricks Community Edition (Serverless) |
| Compute | Serverless — sem All-Purpose Cluster no CE atual |
| Runtime | PySpark 4.1 · Python 3.12 |
| Catálogo | Unity Catalog — catálogo `workspace` |
| Storage | Delta Lake gerenciado pelo UC |
| Dev | VS Code + Claude Code · Windows/PowerShell |

---

## Schemas Delta (Unity Catalog)

```
workspace.bronze   → tabelas: ipca, boi_gordo
workspace.silver   → tabelas: economia
workspace.gold     → tabelas: indicadores
```

Notação obrigatória: `workspace.schema.tabela` (três partes).

---

## Regras PySpark (OBRIGATÓRIAS)

```python
# CORRETO — sempre importar como F
import pyspark.sql.functions as F

# CORRETO — criar DataFrame via createDataFrame
df = spark.createDataFrame(df_pandas, schema=schema)

# CORRETO — collect() apenas para scalares
valor = df.select(F.corr("a", "b")).collect()[0][0]

# CORRETO — toPandas() APENAS para matplotlib
df_plot = df.toPandas()

# CORRETO — try_to_date para dados CEPEA (cabeçalhos viram null)
F.expr("try_to_date(data_raw, 'dd/MM/yyyy')")

# ERRADO — explode em dados CEPEA com cabeçalho
F.to_date(F.col("data_raw"), "dd/MM/yyyy")  # lança CANNOT_PARSE_TIMESTAMP

# ERRADO — acesso JVM direto (não funciona no Serverless)
spark.sparkContext.appName   # lança JVM_ATTRIBUTE_NOT_SUPPORTED
dbutils.fs.ls(...)           # lança JVM_ATTRIBUTE_NOT_SUPPORTED
dbutils.fs.mkdirs(...)       # lança JVM_ATTRIBUTE_NOT_SUPPORTED
```

---

## Constraints Databricks CE Serverless (todos descobertos em execução)

1. **Sem All-Purpose Cluster** — CE atual redireciona Compute para SQL Warehouses. Notebooks rodam via Serverless.
2. **Sem `sparkContext`** — qualquer acesso JVM direto lança `JVM_ATTRIBUTE_NOT_SUPPORTED`.
3. **Sem `dbutils.fs`** — nem `ls()`, nem `mkdirs()`. Verificar arquivos via `os.path.exists()`.
4. **Unity Catalog obrigatório** — `CREATE SCHEMA` sem `LOCATION`. UC gerencia storage.
5. **Catálogo `main` não existe** — descobrir via `SHOW CATALOGS`. Neste workspace: `workspace`.
6. **Upload via Volumes** — não mais DBFS. Path: `/Volumes/workspace/bronze/arquivos/`.
7. **`saveAsTable()` sem path** — nunca usar `.option("path", ...)`.

---

## Normalização de Datas (ADR-002)

```
IPCA:      "dd/MM/yyyy"  →  to_date()      →  date_format("yyyy-MM")
Boi Gordo: dados diários →  try_to_date()  →  date_format("yyyy-MM")  →  groupBy+avg()
Join:      string "yyyy-MM" == string "yyyy-MM"
Pós-join:  to_date(col("join_key"), "yyyy-MM")  →  DateType
```

---

## Estrutura CEPEA (ADR-008)

CSV tem 4 linhas de cabeçalho antes dos dados numéricos:
```
linha 1: INDICADOR DO BOI GORDO CEPEA/ESALQ
linha 2: (vazia)
linha 3: Fonte: Cepea
linha 4: Data | À vista R$ | À vista US$
linha 5+: dados diários desde 23/07/1997
```
Filtro: `.filter(F.col("data_raw").rlike(r"^\d"))` + `try_to_date()`.
Agregação: `groupBy("periodo").agg(avg("valor_boi"))` → média mensal.

---

## Resultado do Pipeline (execução abril/2026)

| Métrica | Valor |
|---|---|
| Período | jan/2024 → fev/2026 (26 meses) |
| Bronze IPCA | 26 meses |
| Bronze Boi Gordo | 28 meses (diários → mensal) |
| Silver overlap | 26 meses · 0 nulls |
| Gold registros | 26 |
| Pearson (var. mensal) | -0.2710 |
| Interpretação | Fraca ou inexistente |

---

## ADRs Vigentes

| ADR | Decisão |
|---|---|
| ADR-001 | CEPEA via CSV manual — scraping descartado |
| ADR-002 | Datas normalizadas via string "yyyy-MM" antes do join |
| ADR-003 | `saveAsTable()` sem path — UC gerencia storage |
| ADR-004 | `F.corr()` via `collect()` — não gravado como tabela |
| ADR-005 | `CREATE SCHEMA` sem `LOCATION` no Unity Catalog |
| ADR-006 | Serverless CE: sem `sparkContext` nem `dbutils.fs` |
| ADR-007 | Catálogo CE: `SHOW CATALOGS` antes de criar schemas. Catálogo real: `workspace` |
| ADR-008 | CEPEA: dados diários desde 1997 — filtrar 2024+ e agregar para média mensal |
| ADR-009 | Upload via Volumes UC: `Catalog → workspace → bronze → Volumes → arquivos` |
| ADR-010 | `try_to_date` em vez de `to_date` para dados CEPEA |

---

## Ordem de Execução

```
config → ipca_bronze_v2 → boi_gordo_bronze_v2 → economia_silver_v2 → insights_gold_v2
```
