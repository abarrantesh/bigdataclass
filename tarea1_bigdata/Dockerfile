# Imagen base con Python y Java 
FROM openjdk:11 


# Instala Spark y Python 
RUN apt-get update && \ 
    apt-get install -y python3 python3-pip && \ 
    pip3 install pyspark pytest 


# Crea estructura
WORKDIR /app 

# Copia el proyecto 
COPY . /app 

# Ejecuta pytest por defecto 
CMD ["/bin/bash","run.sh"]
