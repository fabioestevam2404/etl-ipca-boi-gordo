# LinkedIn Posts — ETL IPCA × Boi Gordo
# Resultado real: Pearson -0.2710 · Executado abril/2026

---

## VARIANTE 1 — Técnica · Para engenheiros de dados

Publiquei um pipeline ETL completo no Databricks que tentou provar uma hipótese econômica.

A hipótese: variações no preço do Boi Gordo se correlacionam positivamente com o IPCA. Faz sentido — carnes pesam ~10% na cesta do IBGE.

O resultado: Pearson = -0.2710. Correlação fraca e negativa. A hipótese não se sustentou nos dados de jan/2024 → fev/2026.

Isso é a parte interessante. Mas o que aprendi durante a execução foi mais valioso do que o resultado.

O Databricks Community Edition mudou bastante. Seis problemas que o spec original não previa, todos resolvidos em produção:

→ Não existe mais All-Purpose Cluster no CE. Notebooks rodam via Serverless direto.

→ `spark.sparkContext` lança `JVM_ATTRIBUTE_NOT_SUPPORTED` no Serverless. Removi tudo que dependia de acesso JVM.

→ `CREATE SCHEMA` com `LOCATION 'dbfs://...'` rejeita com `AnalysisException`. O Unity Catalog gerencia storage internamente — sem LOCATION.

→ O catálogo `main` não existe no CE. Descoberto via `SHOW CATALOGS`. O catálogo real é `workspace`.

→ Upload de arquivos não vai mais para DBFS. Vai para Volumes UC. Path: `/Volumes/workspace/bronze/arquivos/`.

→ O CSV do CEPEA tem 7145 linhas de dados diários desde 1997, com 4 linhas de cabeçalho antes dos números. `to_date()` explodiu na linha "Data". `try_to_date()` resolveu.

Cada um desses virou uma ADR. Ao final, o projeto tem 10 decisões arquiteturais registradas — 4 do design original, 6 da execução real. O CLAUDE.md foi atualizado com todos os constraints.

Esse é o valor do Spec-Driven Development com Claude Code: quando você registra o que descobriu, a próxima sessão começa com esse contexto. Sem re-briefing, sem repetir os mesmos erros.

Stack: PySpark 4.1 · Delta Lake · Databricks CE Serverless · Unity Catalog · BCB API · CEPEA

Código no GitHub: github.com/fabioestevam2404

---
Hashtags:
#DataEngineering #PySpark #Databricks #DeltaLake #Unitycatalog #ETL #Portfolio #SpecDrivenDevelopment

---

## VARIANTE 2 — Narrativa · Para audiência ampla

Fiz uma pergunta simples com dados: o preço do boi gordo explica a inflação brasileira?

Peguei o IPCA de jan/2024 até fev/2026 diretamente da API do Banco Central. Peguei os preços do Boi Gordo do CEPEA/USP — 567 pregões diários transformados em 26 médias mensais.

Rodei um pipeline ETL completo no Databricks, camada por camada: Bronze (dados brutos), Silver (limpeza e join), Gold (variações e correlação).

O resultado: coeficiente de Pearson de -0.2710.

Correlação fraca e negativa. A relação que eu esperava encontrar não está nos dados, pelo menos não de forma direta nesse período.

Algumas possibilidades para isso:

O repasse do boi ao IPCA tem defasagem. O preço da arroba sobe em janeiro e aparece na cesta do consumidor em março. Uma análise com lag de 1-2 meses poderia mostrar algo diferente.

O IPCA de 2024-2025 foi dominado por energia, câmbio e serviços. Carnes pesam ~10% — outros componentes pesam 90%.

As exportações de carne para a China e outros mercados mantiveram o preço doméstico pressionado, mas isso não necessariamente se traduziu em inflação ao consumidor no mesmo compasso.

Dado que não confirma uma hipótese também é dado. Publica-se do mesmo jeito.

O pipeline está no GitHub com código aberto, 10 decisões arquiteturais documentadas e todos os notebooks prontos para replicar.

Link no primeiro comentário.

---
Hashtags:
#IPCA #Commodities #EconomiaBrasileira #Inflacao #Agronegocio #DataAnalytics #BancoCentral #CEPEA #Portfolio

---

## VARIANTE 3 — Metodologia · Para tech leads e sêniors

Terminei um projeto de portfólio no Databricks e o mais valioso não foi o resultado — foi o que quebrou no meio do caminho.

O CE mudou. Seis constraints que o spec original não previa apareceram durante a execução:

Sem All-Purpose Cluster. Sem `sparkContext`. Sem `dbutils.fs`. `CREATE SCHEMA` sem `LOCATION`. Catálogo `main` inexistente. Upload via Volumes, não DBFS.

Cada um parou a execução. Cada um virou uma ADR.

Isso é exatamente o ponto do Spec-Driven Development: você não consegue prever tudo antes de executar. Mas se tiver um processo de registro, o que você descobriu fica disponível para a próxima sessão — seja você ou o Claude Code lendo o CLAUDE.md.

O projeto saiu com 10 ADRs no total. As 4 originais do design. Mais 6 da execução real.

Na próxima vez que alguém rodar esse pipeline no CE — ou que eu precisar de ajuda do Claude Code numa sessão nova — o contexto já está lá. Sem re-briefing. Sem repetir o mesmo erro do `sparkContext`.

O resultado do pipeline em si: Pearson = -0.2710 entre variação mensal do IPCA e do Boi Gordo (jan/2024 → fev/2026). Correlação fraca e negativa — hipótese inicial refutada, o que é igualmente válido como resultado analítico.

Stack: PySpark 4.1 · Unity Catalog · Databricks Serverless · BCB API · CEPEA · Claude Code

GitHub: github.com/fabioestevam2404

Alguém mais está usando ADRs em projetos de dados pessoais? Fico curioso se o hábito de registrar decisões faz diferença no seu fluxo com IA.

---
Hashtags:
#SpecDrivenDevelopment #ClaudeCode #ADR #DataEngineering #Databricks #Portfolio #TechLead #MCP #AIAssistedDevelopment

---

## NOTAS DE PUBLICAÇÃO

Variante 1 → engenheiros de dados, foco nos 6 problemas técnicos do CE
Variante 2 → audiência ampla, foco na pergunta econômica e no resultado
Variante 3 → tech leads e sêniors, foco no processo de registro de decisões

Imagem: screenshot do gráfico dual-axis gerado pelo Gold notebook
Horário ideal: terça a quinta, 7h-9h ou 11h-13h
