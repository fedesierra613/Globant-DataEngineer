# Globant - Data Engineering Challenge: 

Esta es una solución para la Sección 1 del reto de codificación de Ingeniería de Datos de Globant. El proyecto implementa una API REST local para migrar datos desde archivos CSV a una base de datos SQL y permite inserciones de datos por lotes.

**Esta versión utiliza PySpark como motor de procesamiento de datos para demostrar escalabilidad.**

## Arquitectura y Stack Tecnológico

-   **Lenguaje:** Python 3.10
-   **Motor de Procesamiento Distribuido:** Apache Spark (PySpark)
-   **Framework de API:** FastAPI
-   **Base de Datos:** SQLite
-   **ORM:** SQLAlchemy
-   **Servidor ASGI:** Uvicorn
-   **Entorno de Ejecución:** WSL (Ubuntu) con Java 11

## Prerrequisitos

-   WSL (Windows Subsystem for Linux) instalado.
-   Git
-   Python 3.10+ y `pip`
-   Java 11 (OpenJDK)
-   Apache Spark

## Guía de Instalación y Ejecución

1.  **Clonar el repositorio:**
    ```bash
    git clone https://github.com/fedesierra613/Globant-DataEngineer
    cd globant-challenge
    ```

2.  **Instalar Java en WSL:**
    ```bash
    sudo apt update
    sudo apt install -y openjdk-11-jdk
    ```

3.  **Descargar e Instalar Apache Spark:**
    ```bash
    # Descargar Spark y moverlo a /opt/spark
    wget https://archive.apache.org/dist/spark/spark-3.4.1/spark-3.4.1-bin-hadoop3.tgz
    tar xvf spark-3.4.1-bin-hadoop3.tgz
    sudo mv spark-3.4.1-bin-hadoop3 /opt/spark
    ```

4.  **Configurar las Variables de Entorno de Spark:**
    Añade lo siguiente al final de tu archivo `~/.bashrc`:
    ```bash
    export SPARK_HOME=/opt/spark
    export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
    ```
    Luego, recarga tu terminal o ejecuta `source ~/.bashrc`.

5.  **Crear y activar un entorno virtual de Python:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

6.  **Instalar las dependencias de Python:**
    ```bash
    pip install -r requirements.txt
    ```

7.  **Colocar los archivos de datos:**
    Asegúrate de que los archivos `departments.csv`, `jobs.csv` y `hired_employees.csv` estén ubicados en la carpeta `data/`.

8.  **Ejecutar el servidor de la API:**
    ```bash
    uvicorn api.main:app --reload
    ```
    La API estará disponible en `http://127.0.0.1:8000`.

## Endpoints de la API

La documentación interactiva (Swagger UI) está disponible en `http://127.0.0.1:8000/docs`.

### Carga inicial desde CSV (con PySpark)

-   `POST /upload/csv/{table_name}`: Carga el CSV correspondiente a la tabla especificada usando PySpark.
    -   `table_name` puede ser `departments`, `jobs`, o `hired_employees`.

    **Ejemplo con `curl`:**
    ```bash
    curl -X POST http://127.0.0.1:8000/upload/csv/hired_employees
    ```

### Inserción por lotes (Sin cambios)

-   `POST /departments/batch`: Inserta entre 1 y 1000 registros de departamentos.
-   `POST /jobs/batch`: Inserta entre 1 y 1000 registros de trabajos.
-   `POST /employees/batch`: Inserta entre 1 y 1000 registros de empleados.