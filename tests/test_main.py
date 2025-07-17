# tests/test_main.py

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pathlib import Path

from api.main import app
from api.database import Base, get_db  # Importamos la función original que vamos a sobreescribir
from api import models # Necesitamos los modelos para crear las tablas

# --- Configuración de la Base de Datos de PRUEBA ---
# Usamos una ruta absoluta para evitar cualquier ambigüedad.
# La base de datos se creará en el directorio raíz del proyecto como 'test_db.db'.
TEST_DB_PATH = Path(__file__).parent.parent / "test_db.db"
SQLALCHEMY_DATABASE_URL = f"sqlite:///{TEST_DB_PATH}"

# Creamos un motor y una sesión de SQLAlchemy SOLO para las pruebas
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- Sobreescribimos la Dependencia get_db ---
# Esta función se ejecutará en lugar de la original 'get_db' durante las pruebas.
def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()

# Aplicamos la sobreescritura a nuestra app
app.dependency_overrides[get_db] = override_get_db

# --- Nuestra Fixture de Cliente, ahora más potente ---
@pytest.fixture(scope="module")
def client():
    # Antes de que se ejecute cualquier prueba en este archivo:
    # 1. Borramos la base de datos de prueba si existe de una ejecución anterior
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink()
    # 2. Creamos todas las tablas en la base de datos de prueba vacía
    Base.metadata.create_all(bind=engine)

    # 3. Usamos el cliente de prueba, que ahora usará la base de datos de prueba
    with TestClient(app) as test_client:
        # 4. Poblamos la base de datos de prueba con los datos necesarios
        print("\nPoblando base de datos de PRUEBA...")
        test_client.post("/upload/csv/departments")
        test_client.post("/upload/csv/jobs")
        test_client.post("/upload/csv/hired_employees")
        print("Base de datos de prueba poblada.")
        
        # 5. 'yield' pasa el control a las pruebas
        yield test_client
    
    # Después de que todas las pruebas del archivo han terminado:
    print("\nLimpiando base de datos de prueba.")
    Base.metadata.drop_all(bind=engine) # Opcional: Borra las tablas
    if TEST_DB_PATH.exists():
        TEST_DB_PATH.unlink() # Borra el archivo de la base de datos

# --- Los tests no cambian, pero ahora son 100% aislados ---
def test_get_quarterly_hires_report(client):
    response = client.get("/metrics/quarterly_hires")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:
        assert "department" in data[0]

def test_get_departments_above_average(client):
    response = client.get("/metrics/departments_above_average")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)
    if data:
        assert "hired" in data[0]

def test_batch_insert_departments(client):
    new_department_data = [{"id": 999, "department": "Testing Department"}]
    response = client.post("/departments/batch", json=new_department_data)
    assert response.status_code == 200
    assert response.json() == {"message": "Se insertaron 1 departamentos exitosamente."}