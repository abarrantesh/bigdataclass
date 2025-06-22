from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def top_atletas(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("evento").orderBy("tiempo_segundos")
    ranked_df = df.withColumn("rank", row_number().over(window_spec))
    return ranked_df.filter("rank <= 3").select("id_atleta", "evento", "tiempo_segundos", "rank")
