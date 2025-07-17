import findspark
findspark.init()
# =============================================================================



from fastapi import FastAPI, Depends, HTTPException, Body
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

from . import crud, models, schemas
from .database import engine, get_db


models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Globant Data Engineering Challenge API",
    description="API para migrar datos de CSV a BD SQL usando una sesión global de PySpark.",
    version="5.0.0 (findspark)",
)


@app.on_event("startup")
def startup_event():
    """Inicia la sesión global de Spark al arrancar la aplicación."""
    app.state.spark = SparkSession.builder.appName("GlobantFastAPI").getOrCreate()

@app.on_event("shutdown")
def shutdown_event():
    """Detiene la sesión de Spark al apagar la aplicación."""
    app.state.spark.stop()




department_schema = StructType([StructField("id", IntegerType(), True), StructField("department", StringType(), True)])
job_schema = StructType([StructField("id", IntegerType(), True), StructField("job", StringType(), True)])
employee_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("department_id", IntegerType(), True),
    StructField("job_id", IntegerType(), True)
])

@app.post("/upload/csv/{table_name}", tags=["CSV Upload (PySpark)"])
def upload_csv_with_spark(table_name: str, db: Session = Depends(get_db)):
    """Carga datos desde un archivo CSV usando la sesión global de Spark."""
    spark: SparkSession = app.state.spark
    allowed_tables = {
        "departments": (models.Department, "departments.csv", department_schema),
        "jobs": (models.Job, "jobs.csv", job_schema),
        "hired_employees": (models.HiredEmployee, "hired_employees.csv", employee_schema),
    }
    if table_name not in allowed_tables:
        raise HTTPException(status_code=400, detail="Nombre de tabla no válido.")
    model, filename, schema = allowed_tables[table_name]
    filepath = os.path.join("data", filename)
    if not os.path.exists(filepath):
        raise HTTPException(status_code=404, detail=f"Archivo no encontrado: {filepath}")
    try:
        
        df = spark.read.csv(filepath, header=False, schema=schema)
        df = df.toDF(*schema.fieldNames())

        count = crud.bulk_insert_from_spark_df(db, df, model)
        return {"message": f"Se insertaron {count} registros en la tabla '{table_name}' usando PySpark."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocurrió un error al procesar con Spark: {str(e)}")

@app.post("/departments/batch", tags=["Batch Insert"])
def create_departments_batch(departments: List[schemas.DepartmentCreate] = Body(..., max_items=1000), db: Session = Depends(get_db)):
    count = crud.batch_insert(db, departments, models.Department)
    return {"message": f"Se insertaron {count} departamentos exitosamente."}

@app.post("/jobs/batch", tags=["Batch Insert"])
def create_jobs_batch(jobs: List[schemas.JobCreate] = Body(..., max_items=1000), db: Session = Depends(get_db)):
    count = crud.batch_insert(db, jobs, models.Job)
    return {"message": f"Se insertaron {count} trabajos exitosamente."}

@app.post("/employees/batch", tags=["Batch Insert"])
def create_employees_batch(employees: List[schemas.HiredEmployeeCreate] = Body(..., max_items=1000), db: Session = Depends(get_db)):
    count = crud.batch_insert(db, employees, models.HiredEmployee)
    return {"message": f"Se insertaron {count} empleados exitosamente."}


@app.get("/metrics/quarterly_hires", response_model=List[schemas.QuarterlyHires], tags=["Metrics"])
def get_quarterly_hires_report(db: Session = Depends(get_db)):
    """
    Requerimiento 1: Número de empleados contratados para cada trabajo y
    departamento en 2021, dividido por trimestre.
    """
    query = text("""
        WITH hires_2021 AS (
            SELECT
                d.department,
                j.job,
                CAST(strftime('%m', h.datetime) AS INTEGER) AS hire_month
            FROM hired_employees h
            JOIN departments d ON h.department_id = d.id
            JOIN jobs j ON h.job_id = j.id
            WHERE strftime('%Y', h.datetime) = '2021'
        )
        SELECT
            department,
            job,
            SUM(CASE WHEN hire_month BETWEEN 1 AND 3 THEN 1 ELSE 0 END) AS Q1,
            SUM(CASE WHEN hire_month BETWEEN 4 AND 6 THEN 1 ELSE 0 END) AS Q2,
            SUM(CASE WHEN hire_month BETWEEN 7 AND 9 THEN 1 ELSE 0 END) AS Q3,
            SUM(CASE WHEN hire_month BETWEEN 10 AND 12 THEN 1 ELSE 0 END) AS Q4
        FROM hires_2021
        GROUP BY department, job
        ORDER BY department, job;
    """)
    result = db.execute(query).mappings().all()
    return result


@app.get("/metrics/departments_above_average", response_model=List[schemas.DepartmentHires], tags=["Metrics"])
def get_departments_above_average_hires(db: Session = Depends(get_db)):
    """
    Requerimiento 2: Departamentos que contrataron más empleados que la media
    de contrataciones de todos los departamentos en 2021.
    """
    query = text("""
        WITH department_hires_2021 AS (
            -- Paso 1: Contar las contrataciones por departamento en 2021
            SELECT
                department_id,
                COUNT(id) as hired_count
            FROM hired_employees h -- <--- ALIAS AÑADIDO AQUÍ
            WHERE strftime('%Y', h.datetime) = '2021'
            GROUP BY department_id
        ),
        average_hires AS (
            -- Paso 2: Calcular la media de contrataciones a partir del paso anterior
            SELECT AVG(hired_count) as avg_hired
            FROM department_hires_2021
        )
        -- Paso 3: Seleccionar los departamentos cuyo conteo supera la media
        SELECT
            d.id,
            d.department,
            dh.hired_count AS hired
        FROM department_hires_2021 dh
        JOIN departments d ON dh.department_id = d.id
        WHERE dh.hired_count > (SELECT avg_hired FROM average_hires)
        ORDER BY hired DESC;
    """)
    result = db.execute(query).mappings().all()
    return result