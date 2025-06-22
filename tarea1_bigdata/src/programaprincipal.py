from pyspark.sql import SparkSession
from funciones.joiner import join_dataframes
from funciones.agregaciones import promedio_tiempos
from funciones.top import top_atletas

def main():
    spark = SparkSession.builder \
            .appName("AnalisisDeportistas") \
            .getOrCreate()

    #Lectura de CSVs
    df_atletas = spark.read.csv("data/atleta.csv", header=True, inferSchema=True)
    df_nadar = spark.read.csv("data/nadar.csv", header=True, inferSchema=True)
    df_correr = spark.read.csv("data/correr.csv", header=True, inferSchema=True)

    # JOIN entre atletas y disciplinas
    df_nadar_completo = join_dataframes(df_nadar, df_atletas, ['id_atleta'], ['id'])
    df_correr_completo = join_dataframes(df_correr, df_atletas, ['id_atleta'], ['id'])

    # Agregaciones por promedio
    print("\n== Promedios de Natación ==")
    promedio_tiempos(df_nadar_completo).show()

    print("\n== Promedios de Carrera ==")
    promedio_tiempos(df_correr_completo).show()

    # Top atletas con mejores tiempos
    print("\n== Top 3 Nadadores ==")
    top_atletas(df_nadar_completo).show()

    print("\n== Top 3 Corredores ==")
    top_atletas(df_correr_completo).show()

    spark.stop()

if __name__ == "__main__":

    programaprincipal()
