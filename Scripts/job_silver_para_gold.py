
import sys

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# =====================================================
# 1. INICIALIZACAO
# =====================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = "techchallenge-fase3-360988443155"

origem = f"s3://{bucket}/silver/profissionais/"
destino = f"s3://{bucket}/gold/indicadores/"


# =====================================================
# 2. LEITURA DA SILVER
# =====================================================

print("Iniciando leitura da Silver...")

df = spark.read.parquet(origem)

total = df.count()

print(f"Total de registros na Silver: {total}")

if total != 14005:
    raise ValueError(
        f"Quantidade inesperada na Silver: {total}"
    )

campos = [
    "ano_pesquisa",
    "genero",
    "cargo",
    "senioridade",
    "regiao",
    "faixa_salarial",
    "modelo_trabalho"
]

faltantes = [c for c in campos if c not in df.columns]

if faltantes:
    raise ValueError(
        f"Colunas ausentes na Silver: {faltantes}"
    )


# =====================================================
# 3. LIMPEZA DE TEXTOS
# =====================================================

for coluna in campos:

    df = df.withColumn(
        coluna,
        F.when(
            F.trim(F.col(coluna)) == "",
            F.lit(None)
        ).otherwise(F.trim(F.col(coluna)))
    )


# =====================================================
# 4. PADRONIZACAO DOS CARGOS
# =====================================================

print("Padronizando cargos...")

# Preservar a resposta original para auditoria
df = df.withColumn(
    "cargo_original",
    F.col("cargo")
)

cargos_engenharia = [
    "Engenheiro de Dados/Arquiteto de Dados/"
    "Data Engineer/Data Architect",

    "Engenheiro de Dados/"
    "Data Engineer/Data Architect",

    "Arquiteto de Dados/Data Architect"
]

df = df.withColumn(
    "cargo",
    F.when(
        F.col("cargo").isin(cargos_engenharia),
        F.lit("Engenharia e Arquitetura de Dados")
    ).otherwise(F.col("cargo"))
)


# =====================================================
# 5. PADRONIZACAO DA REMUNERACAO
# =====================================================

print("Padronizando faixas salariais...")

# Preservar valores originais
df = df.withColumn(
    "faixa_salarial_original",
    F.col("faixa_salarial")
)

# Criar indicador de correcao
df = df.withColumn(
    "faixa_salarial_corrigida",
    F.lit(False)
)

# Caso 2023: provavel erro de digitacao
condicao_2023 = (
    (F.col("ano_pesquisa") == "2023") &
    (
        F.col("faixa_salarial") ==
        "de R$ 101/mês a R$ 2.000/mês"
    )
)

df = df.withColumn(
    "faixa_salarial_corrigida",
    F.when(condicao_2023, True)
     .otherwise(F.col("faixa_salarial_corrigida"))
)

df = df.withColumn(
    "faixa_salarial",
    F.when(
        condicao_2023,
        F.lit("de R$ 1.001/mês a R$ 2.000/mês")
    ).otherwise(F.col("faixa_salarial"))
)

# Caso 2025-2026: provavel erro de digitacao
condicao_2025 = (
    (F.col("ano_pesquisa") == "2025_2026") &
    (
        F.col("faixa_salarial") ==
        "de R$ 25.001/mês a R$ 3000/mês"
    )
)

df = df.withColumn(
    "faixa_salarial_corrigida",
    F.when(condicao_2025, True)
     .otherwise(F.col("faixa_salarial_corrigida"))
)

df = df.withColumn(
    "faixa_salarial",
    F.when(
        condicao_2025,
        F.lit("de R$ 25.001/mês a R$ 30.000/mês")
    ).otherwise(F.col("faixa_salarial"))
)


# =====================================================
# 6. AUDITORIA DAS CORRECOES
# =====================================================

print("========== AUDITORIA ==========")

df.filter(
    F.col("faixa_salarial_corrigida") == True
).groupBy(
    "ano_pesquisa",
    "faixa_salarial_original",
    "faixa_salarial"
).count().show(
    20,
    truncate=False
)

print("========== FIM AUDITORIA ==========")


# =====================================================
# 7. FUNCAO DE AGRUPAMENTO
# =====================================================

def criar_indicador(nome_tabela, dimensoes):

    print(f"Processando: {nome_tabela}")

    colunas = ["ano_pesquisa"] + dimensoes

    base = df

    # Excluir apenas respostas sem as dimensoes
    # necessarias para este indicador.
    for coluna in dimensoes:
        base = base.filter(
            F.col(coluna).isNotNull()
        )

    agregado = (
        base
        .groupBy(*colunas)
        .agg(
            F.count(F.lit(1)).alias("quantidade")
        )
    )

    # O percentual considera o ano e,
    # quando aplicavel, as dimensoes anteriores.
    particao = ["ano_pesquisa"] + dimensoes[:-1]

    janela = Window.partitionBy(*particao)

    agregado = agregado.withColumn(
        "total_respostas_validas",
        F.sum("quantidade").over(janela)
    )

    agregado = agregado.withColumn(
        "percentual",
        F.round(
            F.col("quantidade") * 100.0 /
            F.col("total_respostas_validas"),
            2
        )
    )

    caminho = destino + nome_tabela + "/"

    (
        agregado
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet(caminho)
    )

    print(f"Tabela gerada: {nome_tabela}")
    print(f"Local: {caminho}")
    print(f"Categorias: {agregado.count()}")


# =====================================================
# 8. GERAR AS SEIS TABELAS GOLD
# =====================================================

criar_indicador(
    "perfil_genero",
    ["genero"]
)

criar_indicador(
    "perfil_cargo",
    ["cargo"]
)

criar_indicador(
    "perfil_senioridade",
    ["senioridade"]
)

criar_indicador(
    "perfil_regiao",
    ["regiao"]
)

criar_indicador(
    "perfil_remuneracao",
    ["senioridade", "faixa_salarial"]
)

criar_indicador(
    "perfil_modelo_trabalho",
    ["modelo_trabalho"]
)


# =====================================================
# 9. FINALIZACAO
# =====================================================

print("===================================")
print("CAMADA GOLD ATUALIZADA COM SUCESSO!")
print("6 TABELAS ANALITICAS GERADAS")
print(f"Destino: {destino}")
print("===================================")

job.commit()
