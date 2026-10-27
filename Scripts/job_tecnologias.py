
import sys
from functools import reduce

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


# ===================================================
# 1. INICIALIZACAO
# ===================================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = "techchallenge-fase3-360988443155"

silver_destino = (
    f"s3://{bucket}/silver/tecnologias/"
)

gold_destino = (
    f"s3://{bucket}/gold/indicadores/"
    "perfil_tecnologias/"
)


# ===================================================
# 2. CONFIGURACAO DAS PESQUISAS
# ===================================================

pesquisas = {
    "2023": {
        "caminho": f"s3://{bucket}/bronze/2023/*.csv",
        "sql": "('P4_d_1 ', 'SQL')",
        "python": "('P4_d_3 ', 'Python')"
    },
    "2024": {
        "caminho": f"s3://{bucket}/bronze/2024/*.csv",
        "sql": "4.d.1_SQL",
        "python": "4.d.3_Python"
    },
    "2025_2026": {
        "caminho": (
            f"s3://{bucket}/bronze/2025_2026/*.csv"
        ),
        "sql": "4.c.1_SQL",
        "python": "4.c.3_Python"
    }
}


# ===================================================
# 3. FUNCAO DE LEITURA
# ===================================================

def ler_csv(caminho):
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("encoding", "UTF-8")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .csv(caminho)
    )


# ===================================================
# 4. PROCESSAR PESQUISA
# ===================================================

def processar_pesquisa(ano, config):

    print(f"Processando tecnologias: {ano}")

    df = ler_csv(config["caminho"])

    for tecnologia in ["sql", "python"]:
        if config[tecnologia] not in df.columns:
            raise ValueError(
                f"Coluna ausente: {ano} - "
                f"{config[tecnologia]}"
            )

    # Selecionar as duas colunas da pesquisa
    base = df.select(
        F.col(f"`{config['sql']}`").alias("SQL"),
        F.col(f"`{config['python']}`").alias("Python")
    )

    # Converter cada tecnologia em linha
    registros = []

    for tecnologia in ["SQL", "Python"]:

        resposta = F.trim(F.col(tecnologia))

        tratado = base.select(
            F.lit(ano).alias("ano_pesquisa"),
            F.lit(tecnologia).alias("tecnologia"),
            resposta.alias("resposta_original")
        )

        tratado = tratado.withColumn(
            "classificacao",
            F.when(
                F.col("resposta_original").isNull()
                | (F.col("resposta_original") == ""),
                F.lit("Nao respondeu")
            ).when(
                F.col("resposta_original") == "1",
                F.lit("Utiliza")
            ).when(
                F.col("resposta_original") == "0",
                F.lit("Nao utiliza")
            ).otherwise(
                F.lit("Verificar")
            )
        )

        registros.append(tratado)

    return registros[0].unionByName(registros[1])


# ===================================================
# 5. CONSOLIDAR AS TRES PESQUISAS
# ===================================================

dataframes = []

for ano, config in pesquisas.items():
    dataframes.append(
        processar_pesquisa(ano, config)
    )

df_silver = reduce(
    lambda a, b: a.unionByName(b),
    dataframes
)

total = df_silver.count()

print(f"Total de registros de tecnologia: {total}")

# Cada participante gera dois registros:
# um para SQL e outro para Python.
if total != 28010:
    raise ValueError(
        f"Total inesperado: {total}"
    )


# ===================================================
# 6. AUDITORIA
# ===================================================

print("========== AUDITORIA TECNOLOGIAS ==========")

df_silver.groupBy(
    "ano_pesquisa",
    "tecnologia",
    "classificacao"
).count().orderBy(
    "ano_pesquisa",
    "tecnologia",
    "classificacao"
).show(50, truncate=False)

invalidos = df_silver.filter(
    F.col("classificacao") == "Verificar"
)

if invalidos.count() > 0:

    invalidos.groupBy(
        "ano_pesquisa",
        "tecnologia",
        "resposta_original"
    ).count().show(50, truncate=False)

    raise ValueError(
        "Respostas desconhecidas nas tecnologias"
    )


# ===================================================
# 7. GRAVAR SILVER
# ===================================================

(
    df_silver
    .write
    .mode("overwrite")
    .partitionBy("ano_pesquisa")
    .parquet(silver_destino)
)

print("Silver de tecnologias criada")


# ===================================================
# 8. CRIAR TABELA GOLD
# ===================================================

agregado = (
    df_silver
    .groupBy(
        "ano_pesquisa",
        "tecnologia",
        "classificacao"
    )
    .agg(
        F.count("*").alias("quantidade")
    )
)

totais = (
    df_silver
    .groupBy(
        "ano_pesquisa",
        "tecnologia"
    )
    .agg(
        F.count("*").alias("total_participantes"),
        F.sum(
            F.when(
                F.col("classificacao").isin(
                    "Utiliza", "Nao utiliza"
                ),
                1
            ).otherwise(0)
        ).alias("total_respostas_validas")
    )
)

df_gold = agregado.join(
    totais,
    on=["ano_pesquisa", "tecnologia"],
    how="inner"
)

# Percentual sobre todos os participantes
df_gold = df_gold.withColumn(
    "percentual_total",
    F.round(
        F.col("quantidade") * 100.0
        / F.col("total_participantes"),
        2
    )
)

# Percentual sobre respostas validas
df_gold = df_gold.withColumn(
    "percentual_respostas_validas",
    F.when(
        F.col("classificacao").isin(
            "Utiliza", "Nao utiliza"
        ) &
        (F.col("total_respostas_validas") > 0),
        F.round(
            F.col("quantidade") * 100.0
            / F.col("total_respostas_validas"),
            2
        )
    ).otherwise(
        F.lit(None).cast("double")
    )
)


# ===================================================
# 9. GRAVAR GOLD
# ===================================================

(
    df_gold
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(gold_destino)
)

print("========== RESULTADO GOLD ==========")

df_gold.orderBy(
    "ano_pesquisa",
    "tecnologia",
    "classificacao"
).show(50, truncate=False)


# ===================================================
# 10. FINALIZACAO
# ===================================================

print("======================================")
print("PROCESSAMENTO TECNOLOGIAS CONCLUIDO!")
print(f"Silver: {silver_destino}")
print(f"Gold: {gold_destino}")
print("======================================")

job.commit()
