import time
import json
import random
from kafka import KafkaProducer

# Lista de dispositivos eléctricos
dispositivos = [
    "Aire acondicionado",
    "Nevera",
    "Televisor",
    "Lavadora",
    "Computador"
]

# Función para generar datos simulados
def generar_consumo():
    return {
        "id_hogar": random.randint(1, 5),
        "dispositivo": random.choice(dispositivos),
        "consumo_kwh": round(random.uniform(0.1, 5.0), 2),
        "timestamp": int(time.time())
    }

# Configuración del productor Kafka
producer = KafkaProducer(
    bootstrap_servers=['127.0.0.1:9092'],  # conexión segura
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Envío continuo de datos
while True:
    dato = generar_consumo()
    producer.send('consumo_energia', value=dato)
    print(f"Enviado: {dato}")
    time.sleep(1)
