from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Configuración de Spark para conectar con Kafka
# Este es el estándar que piden en Suiza: Spark + Kafka + NoSQL
spark = SparkSession.builder \
    .appName("FinancialFraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Definición del Esquema (Data Contract)
# Muy importante en banca para asegurar la integridad de los datos
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("location", StringType(), True)
])

# 3. Lectura del Stream de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "banking-transactions") \
    .load()

# Convertimos los datos binarios de Kafka a JSON legible
transactions = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# 4. LÓGICA DE DETECCIÓN DE FRAUDE (Nivel Senior)
# Usamos una "Ventana de Tiempo" (Window) para detectar ataques de velocidad
fraud_velocity = transactions \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(
        window(col("timestamp"), "1 minute"), 
        col("account_id")
    ) \
    .agg(
        count("transaction_id").alias("tx_count"),
        sum("amount").alias("total_amount")
    ) \
    .filter("tx_count > 3") # REGLA: Más de 3 transacciones en 1 min = FRAUDE

# 5. Escritura de Alertas en Consola (y luego a MongoDB)
query = fraud_velocity.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

print("🚀 Sistema de Detección de Fraude iniciado...")
query.awaitTermination()