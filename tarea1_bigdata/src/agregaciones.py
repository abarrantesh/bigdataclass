from pyspark.sql import DataFrame
from pyspark.sql.functions import avg

def promedio_tiempos(df: DataFrame) -> DataFrame:
    return df.groupBy("id_atleta","evento").agg(avg("tiempo_segundos").alias("promedio_tiempo"))
