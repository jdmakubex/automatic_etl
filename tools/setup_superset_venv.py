"""
Script para inicializar el entorno virtual de Superset y sus dependencias.
Este script se ejecuta desde el servicio superset-venv-setup en Docker Compose.
Toda la documentación y mensajes están en español.
"""
import os
import subprocess
import sys

VENV_PATH = "/app/superset_home/.venv"
REQUIREMENTS = [
    "apache-superset==3.1.0",
    "clickhouse-connect",
    "clickhouse-sqlalchemy",
    "python-dotenv",
    "requests",
    "SQLAlchemy",
    "PyMySQL",
    "pandas",
    "numpy"
]


def run(cmd, msg=None):
    if msg:
        print(f"[SETUP] {msg}")
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Falló el comando: {' '.join(cmd)}")
        print(f"[ERROR] Código de salida: {e.returncode}")
        print(f"[ERROR] Mensaje: {e}")
        sys.exit(1)


def main():
    print("[SETUP] Iniciando entorno virtual de Superset...")
    # Instalar g++ y build-essential para dependencias nativas ANTES de crear el entorno virtual
    run(["apt-get", "update"], "Actualizando repositorios de paquetes del sistema...")
    run(["apt-get", "install", "-y", "g++", "build-essential"], "Instalando g++ y build-essential...")

    # Crear entorno virtual si no existe
    if not os.path.exists(VENV_PATH):
        run([sys.executable, "-m", "venv", VENV_PATH], "Creando entorno virtual en /app/superset_home/.venv")
    if not os.path.exists(os.path.join(VENV_PATH, "bin", "activate")):
        print("[ERROR] El entorno virtual no se creó correctamente. No se encontró el archivo activate.")
        sys.exit(1)

    # Instalar/actualizar pip en el entorno virtual
    pip_path = os.path.join(VENV_PATH, "bin", "pip")
    run([os.path.join(VENV_PATH, "bin", "python"), "-m", "pip", "install", "--upgrade", "pip"], "Actualizando pip en el entorno virtual...")

    # Instalar dependencias de Superset y adicionales
    for pkg in REQUIREMENTS:
        run([pip_path, "install", "--no-cache-dir", pkg], f"Instalando {pkg}")

    # Instalar dependencias adicionales desde requirements.txt si existe
    req_path = "/app/tools/requirements.txt"
    if os.path.exists(req_path):
        run([pip_path, "install", "--no-cache-dir", "-r", req_path], "Instalando dependencias adicionales desde requirements.txt...")

    print("[SETUP] Entorno virtual y dependencias de Superset instaladas correctamente.")
    print("[SETUP] Servicio superset-venv-setup finalizado y listo. Puedes continuar con el arranque de Superset.")

if __name__ == "__main__":
    main()
