import findspark
findspark.init()
# =============================================================================



from fastapi import FastAPI, Depends, HTTPException, Body
from sqlalchemy.orm import Session
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