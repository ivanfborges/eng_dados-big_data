# Databricks notebook source

# MAGIC %md
# MAGIC
# MAGIC ## Arquitetura proposta
# MAGIC
# MAGIC * DBFS (Databricks File System): Optei pelo DBFS por ser o sistema de arquivos padrão no ambiente Databricks. Ele fornece escalabilidade, disponibilidade e integração perfeita com o ecossistema Databricks, facilitando a gestão e o armazenamento de dados de maneira distribuída.
# MAGIC
# MAGIC * Formato Parquet e Delta Lake: Escolhi armazenar meus dados no formato Parquet devido à sua compressão eficiente e à capacidade de leitura otimizada, o que reduz o tempo de acesso aos dados. Além disso, utilizei o Delta Lake para garantir transações ACID (Atomicidade, Consistência, Isolamento, Durabilidade) e controle de versão dos dados, possibilitando operações de data versioning, rollbacks e otimizações como Z-Ordering e Delta Compaction.
# MAGIC
# MAGIC * Arquitetura Distribuída com Apache Spark: A seleção do Apache Spark para processamento paralelo foi baseada na necessidade de lidar com grandes volumes de dados. O Spark oferece capacidades de processamento distribuído, permitindo a execução de operações complexas em paralelo, o que resulta em melhor desempenho e escalabilidade para consultas e análises sobre os dados distribuídos.
# MAGIC
# MAGIC Essa combinação de tecnologias proporciona uma base sólida para a nova arquitetura, garantindo eficiência, confiabilidade e capacidade de escala para lidar com os requisitos do projeto.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Implementação da Arquitetura
# MAGIC **Configuração de Sistemas Distribuídos:** A integração de sistemas de gerenciamento de banco de dados distribuídos no ambiente Databricks envolve a configuração de clusters do Spark para processamento distribuído. É crucial ajustar o tamanho e as especificações desses clusters para suportar a carga de trabalho esperada, garantindo assim a escalabilidade e o desempenho adequado para processamento de grandes volumes de dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modelagem do Novo Banco de Dados
# MAGIC **Delta Lake para Modelagem:** Utilizei o Delta Lake para otimizar a manipulação dos dados do "Cadastro Ambiental Rural". O Delta Lake oferece recursos transacionais, controle de versão e suporte para operações atômicas, garantindo a consistência e a integridade dos dados. Além disso, aproveitei as funcionalidades de Delta Lake para aprimorar a eficiência das consultas e operações no ambiente distribuído.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Projeto de Particionamento Adequado/Efetivo
# MAGIC **Estratégia de Particionamento:** Não vi necessidade em desenvolver um esquema de particionamento, mas poderia ser feito considerando colunas-chave (como 'uf', 'registro_car',	'situacao_cadastro',	'condicao_cadastro' ou outras) para distribuir os dados de forma eficiente entre os nós do banco de dados distribuído. A estratégia de particionamento selecionada deve ser baseada em colunas relevantes para as consultas frequentes, buscando minimizar a necessidade de leitura de partições desnecessárias e otimizar o desempenho das consultas.

# COMMAND ----------

# MAGIC %md
# MAGIC A configuração específica e a integração de sistemas de gerenciamento de banco de dados distribuídos no Databricks podem variar significativamente de acordo com as necessidades do projeto e a infraestrutura envolvida. Essas tarefas envolvem configurações mais específicas e abrangentes no ambiente Databricks, considerando clusters, ajustes de desempenho e integrações com sistemas externos, o que pode exigir uma abordagem mais detalhada e específica dependendo do caso de uso e das tecnologias envolvidas.

# COMMAND ----------

# Caminho para o arquivo Parquet no DBFS
caminho_arquivo = "/FileStore/big-data_project/temas_ambientais-1.parquet"

# Lendo o arquivo Parquet usando o Spark
df = spark.read.parquet(caminho_arquivo)

# Exibindo o conteúdo do DataFrame
df.display()

# COMMAND ----------

# Caminho para salvar a tabela Delta no DBFS
caminho_delta = "/FileStore/big-data_project/delta/temas_amb"

# COMMAND ----------

# Escrevendo o DataFrame como Tabela Delta
df.write.format("delta").mode("overwrite").save(caminho_delta)

# COMMAND ----------

# Lendo a tabela Delta
df_delta = spark.read.format("delta").load(caminho_delta)
df_delta.display()

# COMMAND ----------

# Registrando a tabela para uso em consultas SQL
df_delta.createOrReplaceTempView("tabela_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bateria de Testes (Queries)

# COMMAND ----------

import pandas as pd
import time

# Leitura dos dados em um DataFrame Pandas
df_pd = df_delta.toPandas()

# COMMAND ----------

# Consulta 1 - Distribuído (Spark)
start_time_spark_1 = time.time()
consulta1_spark = spark.sql("""
    SELECT uf, SUM(area_do_imovel) AS area_total_hectares
    FROM tabela_delta
    WHERE uf IN ('MS', 'MT') 
    GROUP BY uf 
    ORDER BY area_total_hectares DESC
""")
#consulta1_spark.display()
end_time_spark_1 = time.time()

# Consulta 1 - Centralizado (Pandas)
start_time_pandas_1 = time.time()
consulta1_pandas = df_pd[df_pd['uf'].isin(['MS', 'MT'])].groupby('uf')['area_do_imovel'].sum().reset_index()
consulta1_pandas = consulta1_pandas.sort_values(by='area_do_imovel', ascending=False)
#display(consulta1_pandas)
end_time_pandas_1 = time.time()

# Cálculo dos tempos de execução
tempo_execucao_spark_1 = end_time_spark_1 - start_time_spark_1
tempo_execucao_pandas_1 = end_time_pandas_1 - start_time_pandas_1

# Exibindo tempos de execução
print(f"Tempo de execução Consulta 1 (Spark): {tempo_execucao_spark_1} segundos")
print(f"Tempo de execução Consulta 1 (Pandas): {tempo_execucao_pandas_1} segundos")

# COMMAND ----------

# Consulta 2 - Distribuído (Spark)
start_time_spark_2 = time.time()
consulta2_spark = spark.sql("""
    SELECT *
    FROM tabela_delta
    WHERE uf IN ('SP', 'RJ', 'MG', 'ES')
""")
#consulta2_spark.display()
end_time_spark_2 = time.time()

# Consulta 2 - Centralizado (Pandas)
start_time_pandas_2 = time.time()
consulta2_pandas = df_pd[df_pd['uf'].isin(['SP', 'RJ', 'MG', 'ES'])]
#display(consulta2_pandas)
end_time_pandas_2 = time.time()

# Cálculo dos tempos de execução
tempo_execucao_spark_2 = end_time_spark_2 - start_time_spark_2
tempo_execucao_pandas_2 = end_time_pandas_2 - start_time_pandas_2

# Exibindo tempos de execução
print(f"Tempo de execução Consulta 2 (Spark): {tempo_execucao_spark_2} segundos")
print(f"Tempo de execução Consulta 2 (Pandas): {tempo_execucao_pandas_2} segundos")

# COMMAND ----------

# Função para verificar se as coordenadas estão dentro do polígono
def check_inside_polygon(lat, lon):
    point = Point(lon, lat)
    return polygon.contains(point)

# Registrando a função como uma UDF (User Defined Function)
spark.udf.register("check_inside_polygon", check_inside_polygon)

# Consulta 3 - Distribuído (Spark)
start_time_spark_3 = time.time()
consulta3_spark = spark.sql("""
    SELECT *
    FROM tabela_delta
    WHERE uf IN ('GO', 'MS', 'MT')
    AND check_inside_polygon(latitude, longitude) = True
""")
#consulta3_spark.display()
end_time_spark_3 = time.time()

# Consulta 3 - Centralizado (Pandas)
start_time_pandas_3 = time.time()
consulta3_pandas = df_pd[df_pd['uf'].isin(['GO', 'MS', 'MT'])]
consulta3_pandas = consulta3_pandas[consulta3_pandas.apply(lambda row: check_inside_polygon(row['latitude'], row['longitude']), axis=1)]
#display(consulta3_pandas)
end_time_pandas_3 = time.time()

# Cálculo dos tempos de execução
tempo_execucao_spark_3 = end_time_spark_3 - start_time_spark_3
tempo_execucao_pandas_3 = end_time_pandas_3 - start_time_pandas_3

# Exibindo tempos de execução
print(f"Tempo de execução Consulta 3 (Spark): {tempo_execucao_spark_3} segundos")
print(f"Tempo de execução Consulta 3 (Pandas): {tempo_execucao_pandas_3} segundos")

# COMMAND ----------

# Consulta 4 - Distribuído (Spark)
start_time_spark_4 = time.time()
consulta4_spark = spark.sql("""
    SELECT YEAR(data_inscricao) AS ano, COUNT(*) AS total_propriedades
    FROM tabela_delta
    GROUP BY YEAR(data_inscricao)
    ORDER BY ano
""")
#consulta4_spark.display()
end_time_spark_4 = time.time()

# Consulta 4 - Centralizado (Pandas)
start_time_pandas_4 = time.time()
df_pd['data_inscricao'] = pd.to_datetime(df_pd['data_inscricao'])
consulta4_pandas = df_pd.groupby(df_pd['data_inscricao'].dt.year).size().reset_index(name='total_propriedades')
consulta4_pandas = consulta4_pandas.rename(columns={'data_inscricao': 'ano'})
#display(consulta4_pandas)
end_time_pandas_4 = time.time()

# Cálculo dos tempos de execução
tempo_execucao_spark_4 = end_time_spark_4 - start_time_spark_4
tempo_execucao_pandas_4 = end_time_pandas_4 - start_time_pandas_4

# Exibindo tempos de execução
print(f"Tempo de execução Consulta 4 (Spark): {tempo_execucao_spark_4} segundos")
print(f"Tempo de execução Consulta 4 (Pandas): {tempo_execucao_pandas_4} segundos")

# COMMAND ----------

# Consulta 5 - Distribuído (Spark)
start_time_spark_5 = time.time()
consulta5_spark = spark.sql("""
    SELECT AVG(area_remanescente_vegetacao_nativa / area_do_imovel) AS percentual_medio
    FROM tabela_delta
""")
#consulta5_spark.display()
end_time_spark_5 = time.time()

# Consulta 5 - Centralizado (Pandas)
start_time_pandas_5 = time.time()
consulta5_pandas = df_pd['area_remanescente_vegetacao_nativa'].mean() / df_pd['area_do_imovel'].mean()
#display(f"Percentual médio (Pandas): {consulta5_pandas}")
end_time_pandas_5 = time.time()

# Cálculo dos tempos de execução
tempo_execucao_spark_5 = end_time_spark_5 - start_time_spark_5
tempo_execucao_pandas_5 = end_time_pandas_5 - start_time_pandas_5

# Exibindo tempos de execução
print(f"Tempo de execução Consulta 5 (Spark): {tempo_execucao_spark_5} segundos")
print(f"Tempo de execução Consulta 5 (Pandas): {tempo_execucao_pandas_5} segundos")


# COMMAND ----------

# Consulta 6 - Distribuído (Spark)
start_time_spark_6 = time.time()
consulta6_spark = spark.sql("""
    SELECT uf, COUNT(*) AS total_propriedades
    FROM tabela_delta
    GROUP BY uf
""")
#consulta6_spark.display()
end_time_spark_6 = time.time()

# Consulta 6 - Centralizado (Pandas)
start_time_pandas_6 = time.time()
consulta6_pandas = df_pd['uf'].value_counts()
#display(f"Total de propriedades por UF (Pandas):\n{consulta6_pandas}")
end_time_pandas_6 = time.time()

# Cálculo dos tempos de execução
tempo_execucao_spark_6 = end_time_spark_6 - start_time_spark_6
tempo_execucao_pandas_6 = end_time_pandas_6 - start_time_pandas_6

# Exibindo tempos de execução
print(f"Tempo de execução Consulta 6 (Spark): {tempo_execucao_spark_6} segundos")
print(f"Tempo de execução Consulta 6 (Pandas): {tempo_execucao_pandas_6} segundos")


# COMMAND ----------

# Consulta 8 - Distribuído (Spark)
start_time_spark_8 = time.time()
consulta8_spark = spark.sql("""
    WITH media_por_estado AS (
        SELECT uf, AVG(area_do_imovel) AS media_area
        FROM tabela_delta
        GROUP BY uf
    )
    SELECT t.uf, COUNT(*) AS propriedades_acima_media
    FROM tabela_delta t
    JOIN media_por_estado m ON t.uf = m.uf
    WHERE t.area_do_imovel > m.media_area
    GROUP BY t.uf
""")
#consulta8_spark.display()
end_time_spark_8 = time.time()

# Consulta 8 - Centralizado (Pandas)
start_time_pandas_8 = time.time()

# Calculando a média de área por estado no DataFrame Pandas
media_por_estado_pandas = df_pd.groupby('uf')['area_do_imovel'].mean().reset_index()
media_por_estado_pandas.columns = ['uf', 'media_area']

# Juntando os DataFrames com a condição e contando as propriedades
joined_df = df_pd.merge(media_por_estado_pandas, on='uf')
consulta8_pandas = joined_df[joined_df['area_do_imovel'] > joined_df['media_area']].groupby('uf').size().reset_index(name='propriedades_acima_media')
#display(f"Propriedades acima da média por UF (Pandas):\n{consulta8_pandas}")

end_time_pandas_8 = time.time()

# Cálculo dos tempos de execução
tempo_execucao_spark_8 = end_time_spark_8 - start_time_spark_8
tempo_execucao_pandas_8 = end_time_pandas_8 - start_time_pandas_8

# Exibindo tempos de execução
print(f"Tempo de execução Consulta 8 (Spark): {tempo_execucao_spark_8} segundos")
print(f"Tempo de execução Consulta 8 (Pandas): {tempo_execucao_pandas_8} segundos")


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Resultados

# COMMAND ----------

# Dados das consultas e tempos de execução
tempos = [
    {"Consulta": "Consulta 1", "Tempo Spark (s)": tempo_execucao_spark_1, "Tempo Pandas (s)": tempo_execucao_pandas_1},
    {"Consulta": "Consulta 2", "Tempo Spark (s)": tempo_execucao_spark_2, "Tempo Pandas (s)": tempo_execucao_pandas_2},
    {"Consulta": "Consulta 3", "Tempo Spark (s)": tempo_execucao_spark_3, "Tempo Pandas (s)": tempo_execucao_pandas_3},
    {"Consulta": "Consulta 4", "Tempo Spark (s)": tempo_execucao_spark_4, "Tempo Pandas (s)": tempo_execucao_pandas_4},
    {"Consulta": "Consulta 5", "Tempo Spark (s)": tempo_execucao_spark_5, "Tempo Pandas (s)": tempo_execucao_pandas_5},
    {"Consulta": "Consulta 6", "Tempo Spark (s)": tempo_execucao_spark_6, "Tempo Pandas (s)": tempo_execucao_pandas_6},
    {"Consulta": "Consulta 8", "Tempo Spark (s)": tempo_execucao_spark_8, "Tempo Pandas (s)": tempo_execucao_pandas_8}
]

# Criando o DataFrame Pandas com os dados
df_tempos = pd.DataFrame(tempos)

# Arredondando os tempos para 4 casas decimais
df_tempos["Tempo Spark (s)"] = round(df_tempos["Tempo Spark (s)"], 4)
df_tempos["Tempo Pandas (s)"] = round(df_tempos["Tempo Pandas (s)"], 4)

# Calculando a diferença entre os tempos
df_tempos["Diferença"] = round(df_tempos["Tempo Spark (s)"] - df_tempos["Tempo Pandas (s)"], 4)

# Exibindo a tabela
df_tempos.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Os resultados indicam consistentemente que o ambiente Spark, por ser distribuído, tende a oferecer tempos de execução mais rápidos em comparação com o ambiente Pandas, especialmente em consultas que envolvem operações mais complexas ou grandes volumes de dados, como nas consultas 3 e 8 em que o tempo foi reduzido em aproximadamente 30 segundos. Consultas que exigem operações paralelizáveis parecem se beneficiar significativamente do ambiente distribuído, enquanto o ambiente centralizado do Pandas tende a enfrentar desafios de escalabilidade para determinados tipos de operações.

# COMMAND ----------

# MAGIC %md
# MAGIC A Consulta 7 não foi testada em ambientes distribuídos e centralizados, uma vez que a natureza da consulta a torna desafiadora de ser comparada diretamente. No caso específico dessa consulta, ela envolve a determinação da maior propriedade e o cálculo da distância até Brasília, ambos baseados em cálculos geoespaciais específicos e envolvem operações que podem ser difíceis de paralelizar efetivamente em um ambiente distribuído sem perder a precisão ou eficiência.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consultas

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Consulta 1: Recupere a soma de área (em hectares) para todas as propriedades agrícolas que pertencem ao MS e MT. Ordene os resultados em ordem decrescente. Essa consulta visa identificar a extensão total das propriedades agrícolas nos estados de Mato Grosso (MT) e Mato Grosso do Sul (MS), apresentando os resultados de forma decrescente.

# COMMAND ----------

# Executando a consulta
consulta1 = spark.sql("""
    SELECT uf, SUM(area_do_imovel) AS area_total_hectares
    FROM tabela_delta
    WHERE uf IN ('MS', 'MT') 
    GROUP BY uf 
    ORDER BY area_total_hectares DESC
""")

# Exibindo os resultados
consulta1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Consulta 2: Filtre todas as propriedades que pertencem à região sudeste. Esta consulta visa extrair informações específicas das propriedades localizadas na região Sudeste do Brasil, proporcionando uma visão detalhada dessas áreas.

# COMMAND ----------

# Executando a consulta
consulta2 = spark.sql("""
    SELECT *
    FROM tabela_delta
    WHERE uf IN ('SP', 'RJ', 'MG', 'ES')
""")

# Exibindo os resultados
consulta2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Consulta 3: Recupere todas as propriedades rurais que estão localizadas dentro de uma área geográfica específica delimitada por um polígono. Esta consulta tem como objetivo identificar todas as propriedades rurais situadas dentro de um polígono descrito pelas seguintes coordenadas: ((-53.5325072 -19.4632582, -51.0495971 -19.1625841, -51.3734501 -16.1924262, -53.8181518 -16.4010783, -53.5325072 -19.4632582))

# COMMAND ----------

pip install shapely

# COMMAND ----------

from shapely.geometry import Point, Polygon
from pyspark.sql import SparkSession

# Criando uma sessão Spark
spark = SparkSession.builder.appName("ConsultaPoligono").getOrCreate()

# Definindo as coordenadas do polígono
coords = [(-53.5325072, -19.4632582), (-51.0495971, -19.1625841), (-51.3734501, -16.1924262), (-53.8181518, -16.4010783), (-53.5325072, -19.4632582)]

# Criando um objeto Polygon (polígono)
polygon = Polygon(coords)

# Função para verificar se as coordenadas estão dentro do polígono
def check_inside_polygon(lat, lon):
    point = Point(lon, lat)
    return polygon.contains(point)

# Registrando a função como uma UDF (User Defined Function)
spark.udf.register("check_inside_polygon", check_inside_polygon)

# Executando a consulta
consulta3 = spark.sql("""
    SELECT *
    FROM tabela_delta
    WHERE uf IN ('GO', 'MS', 'MT')
    AND check_inside_polygon(latitude, longitude) = True
""")

# Exibindo os resultados
consulta3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Consulta 4: Calcule quantas propriedades foram cadastradas por ano. Apresente os resultados em ordem cronológica. Essa consulta tem o propósito de fornecer uma contagem anual do número de propriedades cadastradas, apresentando os resultados em ordem cronológica.

# COMMAND ----------

# Executando a consulta
consulta4 = spark.sql("""
    SELECT YEAR(data_inscricao) AS ano, COUNT(*) AS total_propriedades
    FROM tabela_delta
    GROUP BY YEAR(data_inscricao)
    ORDER BY ano
""")

# Exibindo os resultados
consulta4.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Consulta 5: Calcule o percentual médio de área remanescente de vegetação nativa em comparação à área total da propriedade. Esta consulta busca calcular o percentual médio da área de vegetação nativa que permanece em relação à área total das propriedades rurais, oferecendo insights sobre a preservação ambiental.

# COMMAND ----------

# Executando a consulta
consulta5 = spark.sql("""
    SELECT AVG(area_remanescente_vegetacao_nativa / area_do_imovel) AS percentual_medio
    FROM tabela_delta
""")

# Exibindo os resultados
consulta5.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Consulta 6: Construa uma consulta que mostre a contagem de propriedades rurais por estado. Esta consulta visa fornecer uma visão consolidada do número de propriedades rurais agrupadas por estado, oferecendo uma análise geográfica da distribuição das propriedades.

# COMMAND ----------

# Executando a consulta
consulta6 = spark.sql("""
    SELECT uf, COUNT(*) AS total_propriedades
    FROM tabela_delta
    GROUP BY uf
""")

# Exibindo os resultados
consulta6.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Consulta 7: Veja qual é a maior propriedade entre todas e calcule a distância entre ela e Brasília. Esta consulta tem como objetivo identificar a maior propriedade rural e calcular a distância entre o seu centro e Brasília, utilizando coordenadas geográficas para essa avaliação.

# COMMAND ----------

import math
from pyspark.sql.functions import lit, col

# Encontrar a maior área
maior_area = df.agg({"area_do_imovel": "max"}).collect()[0][0]

# Filtrar para obter a propriedade com a maior área
maior_propriedade = df.filter(col("area_do_imovel") == lit(maior_area)).first()

# Coordenadas de Brasília
brasilia_coords = (-15.826691, -47.921822)  # Substitua pelas coordenadas reais de Brasília

# Coordenadas do centro da maior propriedade
centro_maior_propriedade = (maior_propriedade['latitude'], maior_propriedade['longitude'])

# Função para calcular a distância entre dois pontos usando a fórmula de Haversine
def haversine_distance(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371  # Raio médio da Terra em km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) * math.sin(dlat / 2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) * math.sin(dlon / 2))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = radius * c
    return distance

# Calculando a distância entre a propriedade e Brasília
distancia_para_brasilia = haversine_distance(centro_maior_propriedade, brasilia_coords)

print("A maior propriedade entre todas é:")

# Filtrar para obter a propriedade com a maior área
consulta7 = df_delta.filter(col("area_do_imovel") == lit(maior_area)).select(
    'uf', 'municipio', 'codigo_ibge', 'area_do_imovel', 'registro_car', 'situacao_cadastro', 'condicao_cadastro'
).display()

print("\nDistância até Brasília: %.2f km" % distancia_para_brasilia)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Consulta 8: Faça a média de área entre todas as propriedades. Calcule quantas propriedades por estado estão acima da média. Nesta consulta, busca-se calcular a média da área total das propriedades e, em seguida, determinar quantas propriedades por estado estão acima dessa média, proporcionando insights sobre áreas com tamanhos superiores à média nacional.

# COMMAND ----------

# Executando a consulta
consulta8 = spark.sql("""
    WITH media_por_estado AS (
        SELECT uf, AVG(area_do_imovel) AS media_area
        FROM tabela_delta
        GROUP BY uf
    )
    SELECT t.uf, COUNT(*) AS propriedades_acima_media
    FROM tabela_delta t
    JOIN media_por_estado m ON t.uf = m.uf
    WHERE t.area_do_imovel > m.media_area
    GROUP BY t.uf
""")

# Exibindo os resultados
consulta8.display()
