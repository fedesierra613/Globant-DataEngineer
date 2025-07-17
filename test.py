import sqlite3

DB_FILE = "globant_challenge.db"

# Conectarse a la base de datos
conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

print("--- Contando filas en cada tabla ---")
try:
    cursor.execute("SELECT COUNT(*) FROM departments")
    print(f"Departamentos: {cursor.fetchone()[0]}")

    cursor.execute("SELECT COUNT(*) FROM jobs")
    print(f"Trabajos: {cursor.fetchone()[0]}")

    cursor.execute("SELECT COUNT(*) FROM hired_employees")
    print(f"Empleados: {cursor.fetchone()[0]}")

    print("\n--- Ejemplo de 5 empleados con sus departamentos y trabajos ---")
    query = """
    SELECT
        e.name,
        d.department,
        j.job
    FROM
        hired_employees e
    JOIN
        departments d ON e.department_id = d.id
    JOIN
        jobs j ON e.job_id = j.id
    LIMIT 5;
    """
    for row in cursor.execute(query):
        print(f"Nombre: {row[0]}, Departamento: {row[1]}, Trabajo: {row[2]}")

except Exception as e:
    print(f"Ocurrió un error: {e}")
finally:
    # Cerrar la conexión
    conn.close()