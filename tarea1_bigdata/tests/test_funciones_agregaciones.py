from src.funciones.agregaciones import promedio_tiempos

def test_avg_simple(spark_session):
    # Preparación
    df = spark_session.createDataFrame([
        (1, "100m", 60.0),
        (1, "100m", 70.0),
        ], ["id_atleta","evento", "tiempo_segundos"])
    # Código de prueba
    result = promedio_tiempos(df)
    row = result.collect()[0]
    #Revisión de aserción
    assert row["promedio_tiempo"] == 65.0

def test_avg_multiple_eventos(spark_session):
    # Preparación
    df = spark_session.createDataFrame([
        (1, "100m", 60.0),
        (1, "200m", 80.0),
        ], ["id_atleta","evento", "tiempo_segundos"])
    # Código de prueba
    result = promedio_tiempos(df)
    #Revisión de aserción
    assert result.count() == 2

def test_avg_no_data(spark_session):
    # Preparación
    df = spark_session.createDataFrame([], "id_atleta INT, evento STRING, tiempo_segundos DOUBLE")
    # Código de prueba
    result = promedio_tiempos(df)
    #Revisión de aserción
    assert result.count() == 0
    
def test_avg_single_row(spark_session):
    # Preparación
    df = spark_session.createDataFrame([
        (2, "400m", 55.5),
        ],["id_atleta", "evento", "tiempo_segundos"])
    # Código de prueba
    row = promedio_tiempos(df).collect()[0]
    #Revisión de aserción
    assert row["promedio_tiempo"] == 55.5

def test_avg_multiple_atletas(spark_session):
    # Preparación
    df = spark_session.createDataFrame([
        (1, "100m", 60.0),
        (2, "100m", 62.0),
        (1, "100m", 64.0),
        (2,"100m", 66.0),
        ], ["id_atleta", "evento", "tiempo_segundos"])
    # Código de prueba
    result = promedio_tiempos(df).collect()
    #Revisión de aserción
    assert len(result) == 2
