from sqlalchemy.orm import Session
from pyspark.sql import DataFrame
from typing import List

from . import models, schemas


def bulk_insert_from_spark_df(db: Session, spark_df: DataFrame, model):
    """
    Convierte un DataFrame de Spark en una lista de diccionarios y lo inserta
    en la base de datos usando bulk_insert_mappings de SQLAlchemy.
    """
    # Convertimos el DataFrame de Spark a una lista de diccionarios Python
    records = [row.asDict() for row in spark_df.collect()]

    if not records:
        return 0

    # Insertamos los registros en la base de datos
    db.bulk_insert_mappings(model, records)
    db.commit()
    return len(records)


def batch_insert(db: Session, items: List, model):
    """Inserta un lote de objetos Pydantic en la base de datos (sin cambios)."""
    records = [item.dict() for item in items]
    db.bulk_insert_mappings(model, records)
    db.commit()
    return len(records)