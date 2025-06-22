from src.funciones.top import top_atletas

def test_top3_selection(spark_session):
    # Preparación
    df = spark_session.createDataFrame([
        (1, "100m", 10.5),
        (2, "100m", 10.2),
        (3, "100m", 10.7),
        (4, "100m", 10.1),
        ], ["id_atleta", "evento", "tiempo_segundos"])
    # Código de prueba
    result = top_atletas(df)
    #Revisión de aserción
    assert result.count() == 3

def test_top3_order(spark_session):
    # Preparación
    df = spark_session.createDataFrame([
    (1, "400m", 45.0),
    (2, "400m", 43.0),
    (3, "400m", 44.0),
    ], ["id_atleta", "evento","tiempo_segundos"])
    # Código de prueba
    rows = top_atletas(df).orderBy("rank").collect()
    #Revisión de aserción
    assert rows[0] ["id_atleta"] == 2

def test_top_empty(spark_session):
    # Preparación
    df = spark_session.createDataFrame([], "id_atleta INT, evento STRING, tiempo_segundos DOUBLE")
    # Código de prueba
    result = top_atletas(df)
    #Revisión de aserción
    assert result.count() == 0

def test_top_distintos_eventos(spark_session):
    # Preparación
    df = spark_session.createDataFrame([
        (1, "100m", 10.5),
        (2, "100m", 10.1),
        (3, "200m", 20.5),
        (4, "200m", 20.1),
        ], ["id_atleta", "evento", "tiempo_segundos"])
    # Código de prueba
    result = top_atletas(df)
    eventos = set(row["evento"] for row in result.collect())
    #Revisión de aserción
    assert "100m" in eventos and "200m" in eventos
    
def test_top_mismo_tiempo(spark_session):
    # Preparación
    df = spark_session.createDataFrame([
        (1, "50m", 25.0),
        (2, "50m", 25.0),
        (3, "50m", 25.0),
        (4, "50m", 25.0),
        ], ["id_atleta", "evento", "tiempo_segundos"])
    # Código de prueba
    result = top_atletas(df)
    #Revisión de aserción
    assert result.count() == 3
