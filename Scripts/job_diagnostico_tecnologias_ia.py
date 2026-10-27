
import sys

from pyspark.context import SparkContext
from pyspark.sql import functions as F

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# =========================================
# 1. INICIALIZACAO
# =========================================

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

bucket = "techchallenge-fase3-360988443155"

# =========================================
# 2. ARQUIVOS DE ORIGEM
# =========================================

arquivos = {
    "2023": f"s3://{bucket}/bronze/2023/*.csv",
    "2024": f"s3://{bucket}/bronze/2024/*.csv",
    "2025_2026": f"s3://{bucket}/bronze/2025_2026/*.csv"
}

# =========================================
# 3. CAMPOS IDENTIFICADOS NOS CSVs
# =========================================

mapeamentos = {
    "2023": {
        "uso_ia": "('P4_m ', 'Utiliza ChatGPT ou LLMs no trabalho?')",
        "sql": "('P4_d_1 ', 'SQL')",
        "python": "('P4_d_3 ', 'Python')"
    },
    "2024": {
        "uso_ia": "4.m_usa_chatgpt_ou_copilot_no_trabalho?",
        "sql": "4.d.1_SQL",
        "python": "4.d.3_Python"
    },
    "2025_2026": {
        "uso_ia": "4.j_usa_chatgpt_ou_copilot_no_trabalho?",
        "sql": "4.c.1_SQL",
        "python": "4.c.3_Python"
    }
}

# =========================================
# 4. LEITURA E VALIDACAO
# =========================================

for ano, caminho in arquivos.items():

    print(f"========== PESQUISA {ano} ==========")

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("encoding", "UTF-8")
        .csv(caminho)
    )

    mapa = mapeamentos[ano]

    for indicador, coluna in mapa.items():

        if coluna not in df.columns:
            raise ValueError(
                f"Coluna ausente: {ano} - {coluna}"
            )

        print(f"Indicador: {indicador}")
        print(f"Coluna original: {coluna}")

        resumo = (
            df.groupBy(
                F.col(f"`{coluna}`").alias("resposta")
            )
            .count()
            .orderBy(F.desc("count"))
        )

        # Mostrar as 10 respostas mais frequentes
        resumo.show(10, truncate=120)

    print(f"FIM DA PESQUISA {ano}")

print("===================================")
print("DIAGNOSTICO CONCLUIDO COM SUCESSO")
print("===================================")

job.commit()
