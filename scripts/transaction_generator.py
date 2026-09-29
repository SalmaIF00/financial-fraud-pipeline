import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

# 1. Configuración del Productor (Se conecta al Kafka que acabas de levantar en Docker)
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        api_version=(2, 8, 0),  # <--- AQUÍ ES DONDE SE AÑADE
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("✅ Conectado a Kafka exitosamente.")
except Exception as e:
    print(f"❌ Error conectando a Kafka: {e}")
    exit()

def generate_transaction(account_id=None, amount=None):
    """Genera una transacción financiera realista."""
    return {
        "transaction_id": f"TX-{random.randint(1000000, 9999999)}",
        "account_id": account_id if account_id else f"ACC-{random.randint(100, 999)}",
        "amount": amount if amount else round(random.uniform(5.0, 1000.0), 2),
        "currency": "CHF",  # Moneda suiza para el mercado local
        "location": random.choice(["Zurich", "Geneva", "Bern", "Lugano", "Basel"]),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

print("📡 Iniciando simulación de tráfico bancario...")

try:
    while True:
        # --- CASO 1: Transacción Normal ---
        normal_tx = generate_transaction()
        producer.send('banking-transactions', value=normal_tx)
        
        # --- CASO 2: Fraude de Alto Valor (AML) ---
        # 5% de probabilidad de una transacción sospechosamente alta (> 5000 CHF)
        if random.random() < 0.05:
            high_value_tx = generate_transaction(amount=round(random.uniform(5001, 15000), 2))
            print(f"⚠️ Alerta AML: Enviando transacción de alto valor: {high_value_tx['amount']} CHF")
            producer.send('banking-transactions', value=high_value_tx)

        # --- CASO 3: Fraude de Velocidad (Bot Attack) ---
        # 5% de probabilidad de que una cuenta haga muchas transacciones seguidas
        if random.random() < 0.05:
            fraud_account = "ACC-FRAUD-999"
            print(f"🚨 Alerta Velocity: Generando ráfaga para la cuenta {fraud_account}")
            for _ in range(5):
                burst_tx = generate_transaction(account_id=fraud_account)
                producer.send('banking-transactions', value=burst_tx)
                time.sleep(0.1) # Ráfaga casi instantánea

        time.sleep(1.5) # Pausa entre bloques de transacciones
        
except KeyboardInterrupt:
    print("\n🛑 Simulación detenida por el usuario.")
finally:
    producer.close()