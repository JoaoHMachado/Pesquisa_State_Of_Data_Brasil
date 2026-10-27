
import sys
from functools import reduce

from pyspark.context import SparkContext
from pyspark.sql import functions as F

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

silver_destino = (
    f"s3://{bucket}/silver/uso_ia/"
)

gold_destino = (
    f"s3://{bucket}/gold/indicadores/perfil_uso_ia/"
)


# =====================================================
# 2. CONFIGURACAO DOS ARQUIVOS
# =====================================================

pesquisas = {
    "2023": {
        "caminho": f"s3://{bucket}/bronze/2023/*.csv",
        "coluna": (
            "('P4_m ', "
            "'Utiliza ChatGPT ou LLMs no trabalho?')"
        )
    },
    "2024": {
        "caminho": f"s3://{bucket}/bronze/2024/*.csv",
        "coluna": (
            "4.m_usa_chatgpt_ou_copilot_no_trabalho?"
        )
    },
    "2025_2026": {
        "caminho": (
            f"s3://{bucket}/bronze/2025_2026/*.csv"
        ),
        "coluna": (
            "4.j_usa_chatgpt_ou_copilot_no_trabalho?"
        )
    }
}


# =====================================================
# 3. LEITURA DOS CSVs
# =====================================================

def ler_csv(caminho):

    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(caminho)
    )


# =====================================================
# 4. CLASSIFICACAO DAS RESPOSTAS
# =====================================================

def tratar_ia(ano, configuracao):

    print(f"Processando IA: {ano}")

    df = ler_csv(configuracao["caminho"])

    coluna = configuracao["coluna"]

    if coluna not in df.columns:
        raise ValueError(
            f"Coluna nao encontrada em {ano}: {coluna}"
        )

    df = df.select(
        F.col(f"`{coluna}`").alias("resposta_original")
    )

    df = df.withColumn(
        "ano_pesquisa",
        F.lit(ano)
    )

    resposta = F.lower(
        F.trim(F.col("resposta_original"))
    )

    resposta_vazia = (
        F.col("resposta_original").isNull()
        | (F.trim(F.col("resposta_original")) == "")
    )

    # Resposta negativa
    nao_utiliza = (
        resposta.contains(
            "não utilizo nenhum tipo de solução"
        )
        | resposta.contains(
            "nao utilizo nenhum tipo de solucao"
        )
    )

    # Respostas positivas
    utiliza = (
        resposta.contains("utilizo apenas soluções")
        | resposta.contains("utilizo soluções pagas")
        | resposta.contains("utilizo soluções no estilo")
        | resposta.contains("utilizo soluções de ai")
    )

    # Respostas contraditorias
    conflito = nao_utiliza & utiliza

    df = df.withColumn(
        "classificacao_ia",
        F.when(
            resposta_vazia,
            F.lit("Nao respondeu")
        ).when(
            conflito,
            F.lit("Resposta conflitante")
        ).when(
            nao_utiliza,
            F.lit("Nao utiliza")
        ).when(
            utiliza,
            F.lit("Utiliza IA")
        ).otherwise(
            F.lit("Verificar")
        )
    )

    return df


# =====================================================
# 5. PROCESSAR AS TRES PESQUISAS
# =====================================================

dataframes = []

for ano, configuracao in pesquisas.items():

    tratado = tratar_ia(
        ano,
        configuracao
    )

    dataframes.append(tratado)

df_silver = reduce(
    lambda a, b: a.unionByName(b),
    dataframes
)

total = df_silver.count()

print(f"Total de registros: {total}")

if total != 14005:
    raise ValueError(
        f"Total inesperado: {total}"
    )


# =====================================================
# 6. AUDITORIA DAS CLASSIFICACOES
# =====================================================

print("========== AUDITORIA IA ==========")

df_silver.groupBy(
    "ano_pesquisa",
    "classificacao_ia"
).count().orderBy(
    "ano_pesquisa",
    "classificacao_ia"
).show(30, truncate=False)


# =====================================================
# 7. VALIDAR RESPOSTAS DESCONHECIDAS
# =====================================================

desconhecidas = df_silver.filter(
    F.col("classificacao_ia") == "Verificar"
)

total_desconhecidas = desconhecidas.count()

print(
    f"Respostas desconhecidas: "
    f"{total_desconhecidas}"
)

if total_desconhecidas > 0:

    desconhecidas.groupBy(
        "ano_pesquisa",
        "resposta_original"
    ).count().orderBy(
        F.desc("count")
    ).show(30, truncate=False)

    raise ValueError(
        "Existem respostas nao reconhecidas. "
        "Verifique os logs."
    )


# =====================================================
# 8. VERIFICAR CONFLITOS
# =====================================================

conflitos = df_silver.filter(
    F.col("classificacao_ia")
    == "Resposta conflitante"
)

print("========== CONFLITOS ==========")

conflitos.groupBy(
    "ano_pesquisa"
).count().orderBy(
    "ano_pesquisa"
).show()

print("================================")


# =====================================================
# 9. GRAVAR SILVER COMPLEMENTAR
# =====================================================

(
    df_silver
    .write
    .mode("overwrite")
    .partitionBy("ano_pesquisa")
    .parquet(silver_destino)
)

print("Silver de IA criada com sucesso")


# =====================================================
# 10. GERAR INDICADORES GOLD
# =====================================================

df_gold = (
    df_silver
    .groupBy(
        "ano_pesquisa",
        "classificacao_ia"
    )
    .agg(
        F.count("*").alias("quantidade")
    )
)


# =====================================================
# 11. CALCULAR DENOMINADORES
# =====================================================

# As respostas validas sao:
# Utiliza IA e Nao utiliza.
#
# Nao respondeu e Resposta conflitante
# ficam fora da taxa de adocao.

df_silver = df_silver.withColumn(
    "resposta_valida",
    F.col("classificacao_ia").isin(
        "Utiliza IA",
        "Nao utiliza"
    )
)

totais = (
    df_silver
    .groupBy("ano_pesquisa")
    .agg(
        F.count("*").alias(
            "total_participantes"
        ),

        F.sum(
            F.when(
                F.col("resposta_valida"),
                1
            ).otherwise(0)
        ).alias(
            "total_respostas_validas"
        )
    )
)

df_gold = df_gold.join(
    totais,
    on="ano_pesquisa",
    how="inner"
)


# =====================================================
# 12. CALCULAR PERCENTUAIS
# =====================================================

# Percentual em relacao a todos
# os participantes da pesquisa.

df_gold = df_gold.withColumn(
    "percentual_total",
    F.round(
        F.col("quantidade") * 100.0
        / F.col("total_participantes"),
        2
    )
)

# Percentual entre respostas validas.

df_gold = df_gold.withColumn(
    "percentual_respostas_validas",
    F.when(
        F.col("classificacao_ia").isin(
            "Utiliza IA",
            "Nao utiliza"
        )
        & (F.col("total_respostas_validas") > 0),

        F.round(
            F.col("quantidade") * 100.0
            / F.col("total_respostas_validas"),
            2
        )
    ).otherwise(
        F.lit(None).cast("double")
    )
)


# =====================================================
# 13. GRAVAR GOLD
# =====================================================

(
    df_gold
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(gold_destino)
)

print("========== GOLD IA ==========")

df_gold.orderBy(
    "ano_pesquisa",
    "classificacao_ia"
).show(30, truncate=False)


# =====================================================
# 14. FINALIZACAO
# =====================================================

print("===================================")
print("PROCESSAMENTO IA CONCLUIDO!")
print(f"Total de participantes: {total}")
print(f"Silver: {silver_destino}")
print(f"Gold: {gold_destino}")
print("===================================")

job.commit()
