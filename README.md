# 📊 Tech Challenge — Data Analytics | State of Data Brasil

## 📌 Sobre o projeto

Este projeto tem como objetivo analisar os dados das pesquisas **State of Data Brasil 2023, 2024 e 2025–2026**, buscando identificar tendências no perfil dos profissionais de dados, nas tecnologias utilizadas e na adoção de Inteligência Artificial (IA).

A solução utiliza serviços da **Amazon Web Services (AWS)** para construir um pipeline de dados organizado em camadas, desde a ingestão dos arquivos originais até a disponibilização de tabelas analíticas para consultas e geração de indicadores.

O projeto combina Engenharia de Dados e Análise de Dados para transformar dados brutos em informações que apoiem a interpretação de tendências do mercado de profissionais de dados.

## 🎯 Objetivo

Construir um pipeline de dados em nuvem capaz de processar, padronizar e organizar as informações das pesquisas State of Data Brasil, permitindo comparar indicadores entre os períodos analisados.

### ❓ Pergunta norteadora

> Como evoluíram o perfil dos profissionais de dados, as tecnologias utilizadas e a adoção de Inteligência Artificial entre 2023 e 2025–2026?

## ☁️ Arquitetura da solução

A arquitetura segue o padrão de organização de dados em três camadas: Bronze, Silver e Gold.

```text
Arquivos CSV originais
        │
        ▼
 Amazon S3 — Bronze
        │
        ▼
 AWS Glue — job_bronze_para_silver
        │
        ▼
 Amazon S3 — Silver (Parquet)
        │
        ▼
 AWS Glue — job_silver_para_gold
        │
        ├── 03_uso_ia.py
        └── 04_tecnologias.py
        │
        ▼
 Amazon S3 — Gold
        │
        ▼
 AWS Glue Data Catalog
        │
        ▼
 Amazon Athena
        │
        ▼
 Consultas SQL, indicadores e visualizações
```

### 🥉 Bronze — Dados brutos

Armazena os arquivos CSV originais das pesquisas, preservando os dados de entrada para as etapas seguintes do processamento.

### 🥈 Silver — Dados tratados

Utiliza o AWS Glue com PySpark para realizar o tratamento e a padronização dos dados, preparando os arquivos para as análises posteriores. Os dados tratados são armazenados no formato Parquet.

### 🥇 Gold — Dados analíticos

Disponibiliza tabelas estruturadas para consultas e análises. Nessa etapa são aplicadas regras de negócio, classificações e agregações necessárias para calcular os indicadores do projeto.

As tabelas Gold são catalogadas no AWS Glue Data Catalog e consultadas pelo Amazon Athena.

## 🛠️ Tecnologias e serviços utilizados

| Tecnologia | Aplicação no projeto |
|---|---|
| Amazon S3 | Armazenamento das camadas Bronze, Silver e Gold |
| AWS Glue | Processamento e transformação dos dados |
| PySpark | Tratamento e transformação distribuída |
| AWS Glue Data Catalog | Catalogação e organização das tabelas analíticas |
| Amazon Athena | Consultas SQL sobre os dados armazenados no S3 |
| SQL | Consultas, agregações e análise dos indicadores |
| Python | Desenvolvimento dos processos de tratamento e análise |
| Parquet | Armazenamento dos dados tratados na camada Silver |

## ⚙️ Processamento dos dados

Os principais jobs do pipeline são:

- `01_bronze_para_silver.py` — realiza o processamento dos dados brutos para a camada Silver.
- `02_silver_para_gold.py` — prepara os dados analíticos para a camada Gold.
- `03_uso_ia.py` — aplica regras de classificação e consolidação do indicador de utilização de IA.
- `04_tecnologias.py` — consolida os indicadores relacionados às tecnologias analisadas.

Os jobs são organizados para separar as responsabilidades de tratamento, preparação analítica e cálculo de indicadores.

## 📈 Indicadores analisados

O projeto contempla oito grupos de indicadores:

1. **Gênero:** distribuição dos participantes por gênero.
2. **Cargo:** distribuição dos profissionais por função na área de dados.
3. **Senioridade:** comparação entre níveis Júnior, Pleno, Sênior e Especialista/Staff+.
4. **Região:** distribuição geográfica dos participantes no Brasil.
5. **Remuneração:** análise das faixas salariais por senioridade.
6. **Modelo de trabalho:** comparação entre trabalho remoto, híbrido e presencial.
7. **Uso de Inteligência Artificial:** classificação das respostas sobre utilização de IA.
8. **Tecnologias:** análise do uso declarado de SQL e Python.

## 🤖 Principais resultados

### Adoção de Inteligência Artificial

Entre as respostas válidas classificadas sobre o uso de IA, os resultados foram:

| Pesquisa | Uso declarado de IA |
|---|---:|
| 2023 | 80,83% |
| 2024 | 93,82% |
| 2025–2026 | 98,14% |

Os dados indicam crescimento da utilização declarada de IA entre os respondentes válidos ao longo dos períodos analisados.

**Importante:** respostas ausentes e classificações conflitantes não foram consideradas respostas positivas ou negativas. No período 2025–2026, 1.389 participantes não responderam à classificação de uso de IA. Portanto, o percentual de 98,14% não representa o total de participantes da pesquisa.

### Uso de SQL e Python

| Pesquisa | SQL | Python |
|---|---:|---:|
| 2023 | 83,67% | 74,89% |
| 2024 | 87,66% | 81,78% |
| 2025–2026 | 84,16% | 92,03% |

Os percentuais são calculados sobre as respostas válidas de cada tecnologia. No recorte mais recente, Python apresenta maior percentual de uso declarado do que SQL.

Esses resultados são descritivos e não devem ser interpretados como prova de que uma tecnologia substituiu a outra.

## 📊 Análise e apresentação dos resultados

Os indicadores podem ser utilizados para construir gráficos comparativos, identificar tendências e elaborar uma apresentação executiva.

A análise busca destacar:

- Mudanças no perfil dos profissionais participantes.
- Distribuição de cargos, senioridade e remuneração.
- Preferências de modelo de trabalho.
- Evolução do uso declarado de Inteligência Artificial.
- Comparação entre SQL e Python.
- Implicações para capacitação, engenharia de dados e transformação digital.

## 🧹 Regras de tratamento e qualidade dos dados

Durante a preparação dos indicadores, foram consideradas regras específicas de negócio, incluindo:

- Agrupamento de funções relacionadas à Engenharia e Arquitetura de Dados.
- Manutenção dos níveis Sênior e Especialista/Staff+ como categorias distintas.
- Tratamento de respostas ausentes e conflitantes no indicador de IA.
- Classificação do uso de SQL e Python, preservando a distinção entre respostas positivas, negativas e não informadas.
- Definição dos denominadores utilizados no cálculo dos percentuais.
- Correção de dois valores salariais identificados como prováveis erros de preenchimento.

Essas regras visam melhorar a consistência dos indicadores e tornar a interpretação dos resultados mais transparente.

## 📁 Estrutura sugerida do repositório

```text
tech-challenge-data-analytics/
│
├── README.md
│
├── scripts/
│   ├── 01_bronze_para_silver.py
│   ├── 02_silver_para_gold.py
│   ├── 03_uso_ia.py
│   └── 04_tecnologias.py
│
├── resultados_gold/
│   ├── perfil_genero.csv
│   ├── perfil_cargo.csv
│   ├── perfil_senioridade.csv
│   ├── perfil_regiao.csv
│   ├── perfil_remuneracao.csv
│   ├── perfil_modelo_trabalho.csv
│   ├── perfil_uso_ia.csv
│   └── perfil_tecnologias.csv
│
├── arquitetura/
│   └── Arquitetura_AWS_Tech_Challenge.drawio
│
└── apresentacao/
    └── apresentacao_executiva.pptx
```

*Observação: a estrutura acima é uma sugestão de organização. Ajuste os nomes e os diretórios conforme os arquivos efetivamente publicados no repositório.*

## 🚀 Como reproduzir a solução

A execução completa depende de uma conta AWS com permissões adequadas e dos arquivos de origem.

1. Disponibilizar os CSVs originais no bucket Amazon S3, na camada Bronze.
2. Configurar os caminhos de entrada e saída dos jobs do AWS Glue.
3. Executar o processamento Bronze para Silver.
4. Executar os jobs responsáveis pela geração dos dados analíticos Gold.
5. Configurar ou atualizar as tabelas no AWS Glue Data Catalog.
6. Utilizar o Amazon Athena para consultar as tabelas e validar os indicadores.
7. Exportar os resultados para a elaboração dos gráficos e da apresentação executiva.

Os nomes dos buckets, as regiões AWS, os papéis IAM e os parâmetros de execução devem ser configurados conforme o ambiente utilizado.

## ⚠️ Limitações da análise

- As pesquisas podem apresentar diferenças de questionário, composição da amostra e quantidade de respostas entre os anos.
- Os resultados representam os participantes das pesquisas analisadas e não necessariamente todo o mercado brasileiro de profissionais de dados.
- A ausência de respostas pode afetar a interpretação dos percentuais.
- As comparações são descritivas e não demonstram, por si só, relações de causa e efeito.

## 💡 Conclusão

O projeto demonstra como serviços de Engenharia de Dados em nuvem podem ser utilizados para organizar, tratar e disponibilizar dados de pesquisas para análises comparativas.

A arquitetura com Amazon S3, AWS Glue, Glue Data Catalog e Amazon Athena estabelece um fluxo organizado de processamento, enquanto as tabelas Gold facilitam a consulta aos indicadores e a construção de análises executivas.

Os resultados destacam a elevada utilização declarada de Inteligência Artificial entre os respondentes válidos e a relevância de Python no recorte mais recente, além de oferecer uma visão sobre o perfil profissional, a distribuição regional e os modelos de trabalho.

---

**Projeto desenvolvido como parte de um Tech Challenge de Data Analytics.**

*Dados de origem: pesquisas State of Data Brasil 2023, 2024 e 2025–2026.*
