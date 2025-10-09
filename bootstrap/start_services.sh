#!/bin/bash

# Script para iniciar servicios ETL en secuencia
set -e

echo "🚀 Iniciando servicios ETL en secuencia..."

cd /mnt/c/proyectos/etl_prod

# 1. Primero iniciar ClickHouse solo
echo "📊 Iniciando ClickHouse..."
docker-compose up -d clickhouse
sleep 30

# 2. Esperar a que ClickHouse esté listo
echo "⏳ Esperando a que ClickHouse esté listo..."
while ! docker-compose exec -T clickhouse clickhouse-client --user default --password '' --query "SELECT 1" > /dev/null 2>&1; do
    echo "  Esperando ClickHouse..."
    sleep 5
done
echo "✅ ClickHouse está listo"

# 3. Crear base de datos
echo "📝 Creando base de datos..."
docker-compose exec -T clickhouse clickhouse-client --user default --password '' --query "CREATE DATABASE IF NOT EXISTS etl_prod"

# 4. Inicializar entorno virtual de Superset (en background)
echo "🐍 Configurando entorno virtual de Superset..."
docker-compose up -d superset-venv-setup
sleep 10

# 5. Esperar a que termine la configuración
echo "⏳ Esperando configuración de Superset..."
while docker-compose ps superset-venv-setup | grep -q "running"; do
    echo "  Configurando dependencias..."
    sleep 30
done

# 6. Inicializar Superset
echo "📊 Inicializando Superset..."
docker-compose up -d superset-init
sleep 30

# 7. Iniciar Superset
echo "🌐 Iniciando Superset..."
docker-compose up -d superset
sleep 20

echo "✅ Todos los servicios iniciados"
docker-compose ps