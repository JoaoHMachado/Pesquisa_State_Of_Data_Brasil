
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import sys

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

origens = {
    "2023": f"s3://{bucket}/bronze/2023/*.csv",
    "2024": f"s3://{bucket}/bronze/2024/*.csv",
    "2025_2026": f"s3://{bucket}/bronze/2025_2026/*.csv"
}

destino = f"s3://{bucket}/silver/profissionais/"

# =====================================================
# 2. MAPEAMENTO DAS COLUNAS ORIGINAIS
# =====================================================

mapa_2023 = {
    "id_respondente": "('P0', 'id')",
    "idade": "('P1_a ', 'Idade')",
    "faixa_idade": "('P1_a_1 ', 'Faixa idade')",
    "genero": "('P1_b ', 'Genero')",
    "uf": "('P1_i_1 ', 'uf onde mora')",
    "regiao": "('P1_i_2 ', 'Regiao onde mora')",
    "escolaridade": "('P1_l ', 'Nivel de Ensino')",
    "situacao_trabalho": "('P2_a ', 'Qual sua situação atual de trabalho?')",
    "cargo": "('P2_f ', 'Cargo Atual')",
    "senioridade": "('P2_g ', 'Nivel')",
    "faixa_salarial": "('P2_h ', 'Faixa salarial')",
    "modelo_trabalho": "('P2_r ', 'Atualmente qual a sua forma de trabalho?')",
    "uso_ia": "('P4_m ', 'Utiliza ChatGPT ou LLMs no trabalho?')"
}

mapa_2024 = {
    "id_respondente": "0.a_token",
    "idade": "1.a_idade",
    "faixa_idade": "1.a.1_faixa_idade",
    "genero": "1.b_genero",
    "uf": "1.i.1_uf_onde_mora",
    "regiao": "1.i.2_regiao_onde_mora",
    "escolaridade": "1.l_nivel_de_ensino",
    "situacao_trabalho": "2.a_situação_de_trabalho",
    "cargo": "2.f_cargo_atual",
    "senioridade": "2.g_nivel",
    "faixa_salarial": "2.h_faixa_salarial",
    "modelo_trabalho": "2.r_modelo_de_trabalho_atual",
    "uso_ia": "4.m_usa_chatgpt_ou_copilot_no_trabalho?"
}

mapa_2025 = {
    "id_respondente": "0.a_token",
    "idade": "1.a_idade",
    "faixa_idade": "1.a.1_faixa_idade",
    "genero": "1.b_genero",
    "uf": "1.i.1_uf_onde_mora",
    "regiao": "1.i.2_regiao_onde_mora",
    "escolaridade": "1.l_nivel_de_ensino",
    "situacao_trabalho": "2.a_situação_de_trabalho",
    "cargo": "2.f_cargo_atual",
    "senioridade": "2.g_nivel",
    "faixa_salarial": "2.h_faixa_salarial",
    "modelo_trabalho": "2.q_modelo_de_trabalho_atual",
    "uso_ia": "4.j_usa_chatgpt_ou_copilot_no_trabalho?"
}

mapeamentos = {
    "2023": mapa_2023,
    "2024": mapa_2024,
    "2025_2026": mapa_2025
}

# =====================================================
# 3. LEITURA DOS ARQUIVOS
# =====================================================

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

# =====================================================
# 4. TRATAMENTO E PADRONIZACAO
# =====================================================

def tratar_pesquisa(ano, caminho, mapa):

    print(f"Iniciando tratamento: {ano}")

    df = ler_csv(caminho)

    # Verificar se todas as colunas existem
    faltantes = [
        origem for origem in mapa.values()
        if origem not in df.columns
    ]

    if faltantes:
        raise ValueError(
            f"Colunas nao encontradas em {ano}: {faltantes}"
        )

    # Selecionar e renomear colunas
    expressoes = [
        F.col(f"`{origem}`").alias(destino_coluna)
        for destino_coluna, origem in mapa.items()
    ]

    df = df.select(*expressoes)

    # Limpar textos e transformar vazios em nulos
    for coluna in df.columns:
        df = df.withColumn(
            coluna,
            F.when(
                F.trim(F.col(coluna)) == "",
                F.lit(None)
            ).otherwise(F.trim(F.col(coluna)))
        )

    # Converter idade para inteiro
    df = df.withColumn(
        "idade",
        F.col("idade").cast(IntegerType())
    )

    # Padronizar UF
    df = df.withColumn(
        "uf",
        F.upper(F.col("uf"))
    )

    # Padronizar genero
    genero = F.lower(F.col("genero"))

    df = df.withColumn(
        "genero",
        F.when(genero == "masculino", "Masculino")
         .when(genero == "feminino", "Feminino")
         .otherwise(F.col("genero"))
    )

    # Padronizar modelo de trabalho
    modelo = F.lower(F.col("modelo_trabalho"))

    df = df.withColumn(
        "modelo_trabalho",
        F.when(
            modelo.contains("100% remoto"),
            "Remoto"
        ).when(
            modelo.contains("híbrido") |
            modelo.contains("hibrido"),
            "Hibrido"
        ).when(
            modelo.contains("presencial"),
            "Presencial"
        ).otherwise(F.col("modelo_trabalho"))
    )

    # Identificar ano e edicao de origem
    df = df.withColumn(
        "ano_pesquisa",
        F.lit(ano)
    )

    df = df.withColumn(
        "edicao_pesquisa",
        F.when(
            F.lit(ano) == "2025_2026",
            F.lit("2025-2026")
        ).otherwise(F.lit(ano))
    )

    return df


# =====================================================
# 5. PROCESSAR AS TRES PESQUISAS
# =====================================================

dataframes = []

total_origem = 0

for ano, caminho in origens.items():

    df = tratar_pesquisa(
        ano,
        caminho,
        mapeamentos[ano]
    )

    quantidade = df.count()
    total_origem += quantidade

    print(f"{ano}: {quantidade} registros")

    dataframes.append(df)

# =====================================================
# 6. UNIFICAR OS DATAFRAMES
# =====================================================

df_silver = dataframes[0]

for df in dataframes[1:]:
    df_silver = df_silver.unionByName(df)

total_silver = df_silver.count()

print(f"Total origem: {total_origem}")
print(f"Total Silver: {total_silver}")

if total_silver != total_origem:
    raise ValueError(
        "Quantidade de registros divergente!"
    )

if total_silver != 14005:
    raise ValueError(
        f"Total inesperado: {total_silver}"
    )

# =====================================================
# 7. GRAVAR SILVER EM PARQUET
# =====================================================

(
    df_silver
    .write
    .mode("overwrite")
    .partitionBy("ano_pesquisa")
    .parquet(destino)
)

print("===================================")
print("SILVER CRIADA COM SUCESSO!")
print(f"Destino: {destino}")
print(f"Total de registros: {total_silver}")
print("===================================")

job.commit()
