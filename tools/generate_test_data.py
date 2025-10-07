#!/usr/bin/env python3
"""
Script para generar datos de prueba en Kafka para validar el flujo ETL
"""
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def generate_sample_data():
    """Genera datos de muestra que simulan registros de archivos"""
    return {
        "id": random.randint(1, 1000),
        "nombre": f"archivo_{random.randint(1, 100)}.pdf",
        "fecha_creacion": datetime.now().isoformat(),
        "tipo": random.choice(["expediente", "orden", "acuerdo"]),
        "estado": random.choice(["activo", "archivado", "procesando"]),
        "tamaño": random.randint(1024, 10485760),
        "usuario": f"usuario_{random.randint(1, 10)}",
        "timestamp": int(time.time())
    }

def send_test_data():
    """Envía datos de prueba a Kafka"""
    print("Generando datos de prueba para Kafka...")
    
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: str(x).encode('utf-8')
    )
    
    topic = "test.archivos.data"
    
    for i in range(10):
        data = generate_sample_data()
        key = str(data["id"])
        
        future = producer.send(topic, key=key, value=data)
        result = future.get(timeout=10)
        
        print(f"✅ Enviado registro {i+1}: ID={data['id']}, archivo={data['nombre']}")
        time.sleep(1)
    
    producer.flush()
    producer.close()
    print(f"🎉 Se enviaron 10 registros de prueba al tópico '{topic}'")

if __name__ == "__main__":
    send_test_data()