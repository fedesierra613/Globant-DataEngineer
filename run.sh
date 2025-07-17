#!/bin/bash

# Activa el entorno virtual de Python
source venv/bin/activate

# Exporta las variables de entorno críticas
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # ¡Verifica que esta ruta sea correcta!
export SPARK_HOME=/opt/spark

echo "Entorno configurado. Iniciando Uvicorn..."

# Ejecuta Uvicorn
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload
