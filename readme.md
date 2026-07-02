# Guia de Estudo — Engenharia de Dados: Spark, Databricks e Microsoft Fabric

Caderno temático construído no NotebookLM, como parte do desafio "Crie um Caderno Temático com NotebookLM" da DIO. Este repositório documenta o processo completo: da escolha de fontes até o guia final, passando pelos testes de prompt e as dificuldades encontradas no caminho.

---

## 1. Objetivo

**Assunto:** Engenharia de Dados com foco em Apache Spark, Databricks e Microsoft Fabric.

**Por que esse tema:** Minha stack atual já cobre parte do caminho, mas eu tinha um gap teórico importante em Spark e nos conceitos centrais de arquitetura de dados moderna (lakehouse, medallion, batch vs. streaming). Esse projeto resolve duas coisas ao mesmo tempo: entrega o desafio e preenche esse gap de uma forma mais eficiente.

**Objetivos de estudo:**
- Entender o papel do Apache Spark como motor de processamento distribuído, e como ele aparece tanto na Databricks quanto no Fabric.
- Compreender a arquitetura Medallion (Bronze/Silver/Gold) como padrão de organização de dados em qualquer lakehouse.
- Mapear as diferenças, semelhanças e integrações estruturais entre Databricks e Microsoft Fabric.
- Sair com um vocabulário técnico sólido e prompts reutilizáveis para futuras consultas.

---

## 2. Escolha de Fontes

Fontes abertas, oficiais, selecionadas e carregadas no NotebookLM (via link direto, sem necessidade de conversão para PDF):

| # | Fonte | Link |
|---|---|---|
| 1 | Databricks — Data engineering with Databricks | https://docs.databricks.com/aws/en/data-engineering/ |
| 2 | Databricks — Data engineering concepts (procedural vs. declarative, batch vs. streaming) | https://docs.databricks.com/aws/en/data-engineering/concepts |
| 3 | Databricks — What is the medallion lakehouse architecture? | https://docs.databricks.com/aws/en/lakehouse/medallion |
| 4 | Microsoft Learn — What is Data engineering in Microsoft Fabric? | https://learn.microsoft.com/en-us/fabric/data-engineering/data-engineering-overview |
| 5 | Microsoft Learn — What is Microsoft Fabric? | https://learn.microsoft.com/en-us/fabric/fundamentals/microsoft-fabric-overview |
| 6 | Microsoft Learn — What is a lakehouse in Microsoft Fabric? | https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview |

*Os itens 2 e 3 são páginas internas linkadas a partir da fonte 1; o item 6 é uma página interna linkada a partir da fonte 4. Segui esses links enquanto escolhia porque eles aprofundam exatamente os conceitos que eu precisava (arquitetura e paradigmas de processamento), então optei por incluir como fontes próprias em vez de ficar só na página de entrada.*

---

## 3. Engenharia de Prompts e Cicatrizes

### Prompt 1
**Pergunta:** Explique a diferença entre processamento batch e streaming e me dê um exemplo prático de quando cada um é mais indicado, usando a arquitetura medallion como referência.

**Resposta:** A principal diferença entre o processamento batch (lote) e streaming reside na frequência de processamento e na forma como o motor de execução lida com os dados já processados

**Funcionou bem? Alguma dificuldade?** Funcionou porém senti que precisei refinar o prompt para conseguir um resultado mais satifatório e personalizado para meu estilo de aprendizagem.

### Prompt 2
**Pergunta:** Quais operações de qualidade de dados acontecem na camada silver da arquitetura medallion, e por que elas não acontecem direto na camada bronze?

**Resposta:** Na camada Silver, os dados passam por uma "faxina pesada" para garantir que estejam confiáveis. As principais operações de qualidade incluem:
Limpeza e Normalização, Deduplicação, Tratamento de Nulos, Enforcement de Schema, Regras de Qualidade..
Por que não fazer isso direto na camada Bronze?
Auditoria e Reprocessamento, Proteção do Pipeline, Performance de Ingestão e adicionar validações complexas nessa etapa tornaria a entrada de dados lenta e cara
Em resumo: a Bronze guarda o que realmente aconteceu na fonte, enquanto a Silver guarda a versão correta e validada desses fatos

**Funcionou bem? Alguma dificuldade?** Ótima resposta, mais detalhada do que coloquei aqui.

### Prompt 3
**Pergunta:** Explique o que são processamentos procedurais e declarativos

**Resposta:** Essa distinção refere-se à forma como você instrui o sistema a processar os dados: se você descreve o passo a passo ou se apenas descreve o resultado final desejado.
No modelo procedural, você define explicitamente os passos exatos de execução, no modelo declarativo, você define apenas o resultado desejado.

**Funcionou bem? Alguma dificuldade?** Sim, bem detalhado.

### Troubleshooting geral
- 
- 

---

## 4. Miniguia de Estudo (Entrega Final)

### 4.1 Visão geral — o fio que conecta as três tecnologias

Spark, Databricks e Microsoft Fabric não são três ferramentas concorrentes isoladas — são camadas que se encaixam:

- **Apache Spark** é o motor de processamento distribuído. É a peça que efetivamente lê, transforma e escreve dados em larga escala, em batch ou streaming.
- **Databricks** é uma plataforma completa construída em cima do Spark (com otimizações próprias, como o motor vetorizado Photon), organizada em torno do conceito de **Lakehouse** — uma arquitetura que junta a flexibilidade de um data lake com a confiabilidade transacional de um data warehouse.
- **Microsoft Fabric** também adota o conceito de Lakehouse, mas dentro de um ecossistema SaaS mais amplo da Microsoft, unificando engenharia de dados, ciência de dados, data warehouse, BI (Power BI) e inteligência em tempo real sob um armazenamento único chamado **OneLake**.

Ambas as plataformas usam **Spark** como motor de processamento e **Delta Lake** como formato de armazenamento das tabelas. A diferença está em *quem organiza o ecossistema em volta do motor* e *como a governança e o armazenamento são centralizados*.

### 4.2 Databricks: como a engenharia de dados é organizada

**Lakeflow — a solução de ponta a ponta.** A Databricks concentra suas ferramentas de engenharia de dados sob o nome Lakeflow, cobrindo ingestão, transformação e orquestração, dividido em quatro componentes:

- **Lakeflow Connect** — ingestão de dados, com conectores gerenciados (interface simples, baixa manutenção) e conectores padrão (acesso mais amplo, direto em pipelines).
- **Lakeflow Spark Declarative Pipelines (SDP)** — framework declarativo (sucessor conceitual do Delta Live Tables) para pipelines batch/streaming. Você declara o resultado desejado e o sistema orquestra a execução. Trabalha com *flows* (processam dados usando a API de DataFrame do Spark), *streaming tables* (tabelas Delta com suporte a processamento incremental), *materialized views* (resultados em cache) e *sinks* (destinos externos, como Kafka).
- **Lakeflow Designer** — ferramenta visual (drag-and-drop) de preparação de dados, com suporte a comandos em linguagem natural (Genie Code). O trabalho visual vira código de produção governado pelo Unity Catalog.
- **Lakeflow Jobs** — orquestração e monitoramento em produção. Um job é composto por *tasks* (notebooks, pipelines, queries SQL, treinamento de ML) e suporta lógica de controle de fluxo (condicionais, laços).

**Databricks Runtime e Apache Spark.** O Databricks Runtime é o ambiente de computação otimizado onde os workloads Spark rodam (batch e streaming), incluindo o Photon (motor de consultas vetorizado) e autoscaling. Os programas Spark podem ser notebooks, JARs ou pacotes Python. O Structured Streaming é o motor do Spark para processamento próximo do tempo real.

### 4.3 Conceitos fundamentais de engenharia de dados

**Processamento procedural vs. declarativo**

| Aspecto | Procedural | Declarativo |
|---|---|---|
| O que você define | Os passos exatos (como fazer) | O resultado desejado (o que fazer) |
| Controle | Total, mas exige mais trabalho manual | O sistema decide o plano de execução |
| Otimização | Manual (tuning feito por você) | Automática (query planning do sistema) |
| Exemplo na prática | Apache Spark, por padrão | Lakeflow Spark Declarative Pipelines |

Procedural vale a pena quando você precisa de controle fino ou regras de negócio difíceis de expressar declarativamente. Declarativo vale a pena quando a prioridade é simplicidade e manutenibilidade.

**Processamento em batch vs. streaming**

- **Batch**: reprocessa todos os dados disponíveis na fonte a cada execução (geralmente particionados para limitar retrabalho).
- **Streaming**: mantém rastreamento do que já foi processado; nas próximas execuções, processa só o que é novo.

Importante: no Spark, streaming não significa necessariamente "tempo real em milissegundos" — o Structured Streaming trata até armazenamento em nuvem e tabelas Delta como fontes de streaming, para ter um processamento incremental eficiente, rodando de forma contínua ou disparada po triggers.

| | Batch | Streaming |
|---|---|---|
| Vantagem | Lógica simples; resultado sempre reflete 100% dos dados | Eficiente (só processa o novo); latência baixa |
| Desvantagem | Reprocessamento desnecessário; latência maior | Lógica complexa em cenários com estado (joins, agregações, deduplicação) |

A complexidade do streaming aparece principalmente em processamento *stateful* (joins, agregações, deduplicação), que sofre mais com dados fora de ordem ou atrasados. Processamento *stateless* (ex: apenas anexar dados novos) lida melhor com isso.

**Recomendação prática por camada** (conecta direto com a arquitetura Medallion abaixo):
- **Bronze**: streaming costuma ser melhor (não há processamento com estado aqui).
- **Silver**: geralmente batch com incremental refresh; streaming só quando latência importa mais que precisão total.
- **Gold**: batch, quase sempre (processamento fortemente stateful, volume menor).

### 4.4 Arquitetura Medallion (Bronze → Silver → Gold)

Padrão de design mais citado em qualquer lakehouse. Organiza os dados em camadas com níveis crescentes de qualidade. Também chamada de multi-hop.

- **Bronze (dado bruto):** ingestão sem tratamento, mantém formato original, cresce por append, não é destinado a analistas, funciona como fonte única da verdade para reprocessamento e auditoria. Recomenda-se armazenar campos como string/VARIANT/binário para não quebrar com mudanças de schema.
- **Silver (dado validado):** limpeza, deduplicação, normalização. Nunca se escreve direto da ingestão pro silver. Contém pelo menos uma versão validada e não-agregada de cada registro. Operações típicas: enforcement de schema, tratamento de nulos, deduplicação, resolução de dados fora de ordem, checagem de qualidade, casting de tipos, joins.
- **Gold (dado pronto para negócio):** visões refinadas e agregadas para dashboards, relatórios, ML. Alinhado à lógica do negócio (modelagem dimensional), otimizado para performance de consulta. É comum ter múltiplas camadas gold por área (RH, financeiro, TI).

**Frequência de ingestão x custo x latência:**
- Incremental contínua: custo mais alto, latência mais baixa.
- Incremental disparada (trigger): custo mais baixo, latência mais alta.
- Batch com incremento manual: custo mais baixo ainda, latência maior, exige mais arquitetura.

### 4.5 Microsoft Fabric: o ecossistema equivalente da Microsoft

**O que é.** É um sistema de ingestão, transformação, streaming em tempo real, análise e relatórios, sobre um modelo compartilhado de computação e armazenamento. Integra Data Engineering, Data Factory, Data Science, Real-Time Intelligence, Data Warehouse e Bases de dados.

**OneLake** É um Data lake centralizado e lógico do Fabric, construído sobre Azure Data Lake Storage Gen2. Equivalente ao papel do Delta Lake/Unity Catalog na Databricks, mas todo tenant já vem com um único OneLake, sem provisionamento, organizado em workspaces (pastas) e lakehouses dentro de cada workspace. O recurso **Shortcut** permite "puxar dados de fontes externas (ADLS, S3, GCS) dentro do OneLake sem copiar os dados fisicamente.

**Componentes relevantes:**
- **Data Engineering:** fornece Apache Spark, notebooks e ferramentas para transformação, integrado ao Data Factory para orquestrar jobs.
- **Data Factory:** integração de dados com 200+ conectores com a simplicidade do Power Query.
- **Data Warehouse:** motor  SQL com armazenamento nativo em Delta Lake, separando procrssamento de armazenamento.
- **Real-Time Intelligence:** análise de dados no momento em que chegam (IoT, logs, cliques).
- **Power BI:** camada de visualização.
- **Data Science:** construção e operacionalização de modelos de ML, integrado ao Azure Machine Learning.

Dentro do módulo de Data Engineering, os objetos principais são: **Lakehouse** (armazenamento estruturado/não-estruturado), **Spark job definition** (define como um job roda em cluster Spark), **Notebook** (ambiente interativo Python/R/Scala) e **Pipeline** (passos de coleta, processamento e transformação).

**Lakehouse vs. Data Warehouse no Fabric** (ambos guardam dado em Delta sobre o OneLake, mas servem propósitos diferentes):

| | Lakehouse | Data Warehouse |
|---|---|---|
| Ferramenta principal | Apache Spark (Python, Scala, SQL, R) | T-SQL |
| Tipos de dado | Estruturados e não-estruturados | Estruturados |
| Transações multi-tabela | Não | Sim |
| Melhor para | Engenharia/ciência de dados, arquiteturas medallion | Relatórios de BI, modelagem dimensional, times SQL-first |

Toda lakehouse gera automaticamente um **SQL analytics endpoint**: consulta as tabelas Delta via T-SQL (somente leitura), conecta direto ao Power BI. Só tabelas em Delta aparecem ali, Parquet/CSV precisam ser convertidos primeiro.

### 4.6 Databricks x Fabric

| Conceito | Databricks | Microsoft Fabric |
|---|---|---|
| Motor de processamento | Apache Spark (Databricks Runtime + Photon) | Apache Spark (nativo do Data Engineering) |
| Formato de armazenamento | Delta Lake | Delta Lake (sobre OneLake) |
| Armazenamento centralizado | Unity Catalog + object storage (S3/ADLS/GCS) | OneLake (único por tenant) |
| Orquestração declarativa | Lakeflow Spark Declarative Pipelines | Dataflows Gen 2 / Pipelines |
| Orquestração/agendamento | Lakeflow Jobs | Data Factory |
| Camada de BI | Databricks SQL / dashboards AI-BI | Power BI |
| Padrão arquitetural | Medallion (Bronze/Silver/Gold) | Mesmo padrão se aplica |
| Consulta SQL sobre dado gerenciado | Databricks SQL | SQL analytics endpoint (somente leitura) |

**Conclusão:** o conhecimento de Spark, Delta Lake e arquitetura Medallion é transferível entre as duas plataformas. O que muda é como cada fornecedor organiza governança, orquestração e integração com o resto do ecossistema.

### 4.7 Glossário

- **Apache Spark:** motor de processamento distribuído para dados em larga escala, batch e streaming.
- **Databricks Runtime:** ambiente de computação otimizado da Databricks para rodar workloads Spark.
- **Photon:** motor de consultas vetorizado nativo da Databricks.
- **Lakeflow:** solução de engenharia de dados da Databricks (ingestão + transformação + orquestração).
- **Lakeflow Connect:** componente de ingestão de dados do Lakeflow.
- **Lakeflow Spark Declarative Pipelines (SDP):** framework declarativo para pipelines batch/streaming; sucessor do Delta Live Tables.
- **Lakeflow Designer:** ferramenta visual de preparação de dados.
- **Lakeflow Jobs:** camada de orquestração e agendamento da Databricks.
- **Flow:** unidade de processamento dentro de um pipeline declarativo.
- **Streaming table:** tabela Delta com suporte a processamento incremental/contínuo.
- **Materialized view:** view com resultado pré-computado e em cache.
- **Sink:** destino externo de dados dentro de um pipeline.
- **Structured Streaming:** motor do Spark para processamento em streaming.
- **Delta Lake:** camada de armazenamento com suporte transacional (ACID) para tabelas do lakehouse.
- **Unity Catalog:** sistema de governança de dados da Databricks.
- **Medallion architecture:** padrão de organização em camadas Bronze/Silver/Gold.
- **Bronze layer:** camada de dados brutos, sem tratamento.
- **Silver layer:** camada de dados validados, limpos e deduplicados.
- **Gold layer:** camada de dados agregados, prontos para consumo de negócio.
- **Processamento procedural:** você define explicitamente os passos de execução.
- **Processamento declarativo:** você define o resultado desejado; o sistema decide como executar.
- **Batch processing:** reprocessa todos os dados disponíveis a cada execução.
- **Streaming processing:** rastreia o que já foi processado, trata só dados novos.
- **Stateful processing:** processamento que mantém estado entre execuções (joins, agregações, deduplicação).
- **Microsoft Fabric:** plataforma de análise SaaS da Microsoft, unificando engenharia de dados, ciência de dados, data warehouse, BI e real-time analytics.
- **OneLake:** data lake lógico centralizado do Fabric, único por tenant.
- **Shortcut (OneLake):** referência de acesso zero-copy a dados fora do OneLake.
- **Lakehouse (Fabric):** item do Fabric combinando armazenamento estruturado/não-estruturado com consulta via Spark e SQL.
- **SQL analytics endpoint:** endpoint somente-leitura de cada lakehouse do Fabric, para T-SQL.
- **Data Factory (Fabric):** serviço de integração/ingestão de dados do Fabric.
- **Dataflows Gen 2:** ferramenta low-code de ingestão e preparação de dados no Fabric.
- **Spark job definition:** definição de um job Spark compilado para ETL em produção no Fabric.

### 4.8 Prompts reutilizáveis (para revisão futura)

1. "Explique a diferença entre processamento batch e streaming e me dê um exemplo prático de quando cada um é mais indicado, usando a arquitetura medallion como referência."
2. "Compare o papel do Unity Catalog na Databricks com o papel do OneLake no Microsoft Fabric."
3. "Quais operações de qualidade de dados acontecem na camada silver da arquitetura medallion, e por que elas não acontecem direto na camada bronze?"
4. "Explique quando eu devo escolher processamento procedural em vez de declarativo em um pipeline de dados."
5. "Monte uma tabela comparando Lakehouse e Data Warehouse dentro do Microsoft Fabric."
6. "Quais são os quatro componentes do Lakeflow da Databricks e o que cada um resolve?"
7. "Como o SQL analytics endpoint do Fabric se relaciona com o Databricks SQL?"

---

## Sobre este repositório

Projeto desenvolvido para o desafio "Crie um Caderno Temático com NotebookLM" da [DIO]. Combina o requisito da entrega com um objetivo real de aprendizagem: preencher lacunas teóricas em Spark, Databricks e Fabric dentro da minha transição para Engenharia de Dados.