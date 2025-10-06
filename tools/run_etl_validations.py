import subprocess
import sys

def run_init_clickhouse_table():
    print("Inicializando tabla de ClickHouse...")
    result = subprocess.run([sys.executable, "tools/init_clickhouse_table.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        print("Error al inicializar la tabla de ClickHouse:", result.stderr)
        sys.exit(1)

def main():
    run_init_clickhouse_table()
    # Aquí puedes agregar llamadas a otros scripts de validación
    # Ejemplo:
    # subprocess.run([sys.executable, "tools/validate_superset.py"])
    # subprocess.run([sys.executable, "tools/validate_clickhouse.py"])
    print("Secuencia de validaciones ETL completada.")

if __name__ == "__main__":
    main()
