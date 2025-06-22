from pyspark.sql import DataFrame

def join_dataframes(df1: DataFrame, df2: DataFrame, key1: list, key2: list) -> DataFrame:
    join_expr = [df1[k1] == df2[k2] for k1, k2 in zip(key1, key2)]
    return df1.join(df2, join_expr, "inner")
