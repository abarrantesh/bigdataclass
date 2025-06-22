from src.funciones.joiner import join_dataframes

def test_one_column_join(spark_session):
    # Preparación
    grades_data = [(1, 50), (2, 100)]
    grades_df = spark_session.createDataFrame(grades_data, ['student_id', 'grade'])

    student_data = [(1,'Juan'), (2, 'Maria')]
    student_df = spark_session.createDataFrame(student_data, ['id', 'name'])
    
    # Código de prueba
    joined = join_dataframes(grades_df, student_df, ['student_id'], ['id'])

    #Revisión de aserción
    expected = spark_session.createDataFrame(
        [(1, 50, 1, 'Juan'), (2, 100, 2, 'Maria')],
        ['student_id', 'grade', 'id', 'name']
        )
    assert joined.collect() == expected.collect()

    
def test_empty_join(spark_session):
    # Preparación
    df1 = spark_session.createDataFrame([],schema="id INT, value STRING")
    df2 = spark_session.createDataFrame([], schema="id INT, desc STRING")
    # Código de prueba
    result = join_dataframes(df1, df2, ['id'], ['id'])
    #Revisión de aserción
    assert result.count() == 0

def test_partial_match(spark_session):
    # Preparación
    df1 = spark_session.createDataFrame([(1, "A"), (2, "B")], ["id", "name"])
    df2 = spark_session.createDataFrame([(2, "Match"), (3, "No")], ["id", "desc"])
    # Código de prueba
    result = join_dataframes(df1, df2, ["id"], ["id"])
    #Revisión de aserción
    assert result.count() == 1


def test_column_mismatch(spark_session):
    # Preparación
    df1 = spark_session.createDataFrame([(1, "foo")], ["x","y"])
    df2 = spark_session.createDataFrame([(1, "bar")], ["a", "b"])
    # Código de prueba
    result = join_dataframes(df1, df2, ["x"], ["a"])
    #Revisión de aserción
    assert "b" in result.columns

def test_multi_key_join(spark_session):
    # Preparación
    df1 = spark_session.createDataFrame([(1, "A", 2023)],["id", "name", "year"])
    df2 = spark_session.createDataFrame([(1, 2023, "OK")], ["id", "year", "status"])
    # Código de prueba
    result = join_dataframes(df1, df2, ["id", "year"], ["id", "year"])
    #Revisión de aserción
    assert result.count() == 1
