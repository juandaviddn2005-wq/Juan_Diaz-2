# Juan_Diaz-2
Trabajo Numero 3 BIG DATA JUAN DIAZ  2
# Tarea 3 – Procesamiento de datos con Apache Spark y Kafka

**Curso:** Big Data (código 202016911A_2204) · Universidad Nacional Abierta y a Distancia (UNAD) · Ingeniería Industrial
**Estudiante:** Juan David Diaz Narvaez

Este repositorio contiene el procesamiento **batch** con PySpark sobre el dataset de comercio electrónico de Olist y el ejercicio de **tiempo real** con Apache Kafka y Spark Structured Streaming (Anexo 3 de la guía).

---

## 1. Problema

¿Los retrasos en la entrega de un pedido afectan la calificación que el cliente deja en su reseña?

Para responderlo, se clasifica cada pedido como **A tiempo** o **Retrasado** (comparando la fecha de entrega con la fecha estimada) y se compara la calificación promedio de cada grupo.

## 2. Dataset

**Brazilian E-Commerce Public Dataset by Olist** (Kaggle): https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Pedidos de un marketplace brasileño (2016–2018) repartidos en 9 archivos CSV (unos 121 MB en total):

`olist_customers_dataset.csv`, `olist_geolocation_dataset.csv`, `olist_order_items_dataset.csv`, `olist_order_payments_dataset.csv`, `olist_order_reviews_dataset.csv`, `olist_orders_dataset.csv`, `olist_products_dataset.csv`, `olist_sellers_dataset.csv` y `product_category_name_translation.csv`.

El análisis usa principalmente las tablas de **pedidos** (`olist_orders_dataset.csv`) y **reseñas** (`olist_order_reviews_dataset.csv`).

> El dataset **no está incluido** en el repositorio por su tamaño. Descárgalo desde el enlace anterior.

## 3. Estructura del proyecto

```
Tarea3_BigData/
├── codigo_batch/
│   ├── analisis_olist.py        # Procesamiento batch con PySpark (limpieza, transformación y EDA)
│   └── graficas_olist.py        # Gráficas a partir de los resultados del batch
├── streaming/
│   ├── kafka_producer.py        # Productor: genera datos de sensores simulados
│   └── spark_streaming_consumer.py   # Consumidor: Spark Streaming leyendo de Kafka
├── datos/                       # CSV de Olist (no incluidos en el repositorio)
└── resultados/                  # Salidas generadas (CSV de Spark y gráficas)
    └── graficas/
```

## 4. Requisitos

Probado en una máquina virtual con **Ubuntu 22.04** (VirtualBox):

- Java 11 (`openjdk-11-jdk-headless`)
- Python 3.10
- PySpark 3.5.3
- matplotlib
- kafka-python (solo para el streaming)
- Apache Kafka 3.9.2 (solo para el streaming)

```bash
sudo apt install -y openjdk-11-jdk-headless python3-pip unzip
python3 -m pip install --user pyspark==3.5.3 matplotlib kafka-python
```

## 5. Procesamiento batch

### 5.1 Preparar los datos

Descarga el dataset de Kaggle y descomprímelo en la carpeta `datos/`:

```bash
mkdir -p ~/Tarea3_BigData/datos
unzip archive.zip -d ~/Tarea3_BigData/datos
```

### 5.2 Ejecutar el análisis

```bash
python3 ~/Tarea3_BigData/codigo_batch/analisis_olist.py
python3 ~/Tarea3_BigData/codigo_batch/graficas_olist.py
```

> Las rutas de los scripts apuntan a `~/Tarea3_BigData`. Si tu carpeta es otra, ajusta las rutas que están al inicio de cada archivo.

El primer script carga los CSV, revisa los esquemas, calcula los días de entrega y de retraso, clasifica cada pedido, une los pedidos con sus reseñas y guarda los resultados como CSV en `resultados/`. El segundo script lee esos resultados y genera las gráficas.

### 5.3 Resultados

| Estado de entrega | Pedidos | Días promedio de entrega | Calificación promedio |
|---|---:|---:|---:|
| Retrasado | 6.535 | 33,91 | 2,27 |
| A tiempo | 89.941 | 10,94 | 4,29 |

- Total de pedidos: **99.441**. Total de reseñas: **104.162**.
- Los pedidos retrasados reciben una calificación promedio cercana a **2 puntos menos** que los entregados a tiempo.
- La calificación se calcula **por reseña, no por pedido**: 547 pedidos tienen más de una reseña (hasta 3), por eso el conteo de reseñas "a tiempo" (89.949) es algo mayor al de pedidos (89.941).
- 2.965 pedidos no entran en ninguna categoría porque no se pudo calcular su estado de entrega.

### 5.4 Gráficas

Se generan en `resultados/graficas/`:

- `pedidos_por_estado.png`
- `calificacion_por_estado.png`

## 6. Tiempo real: Kafka + Spark Streaming (Anexo 3)

Un productor envía datos simulados de sensores (`sensor_id`, `temperature`, `humidity`, `timestamp`) al *topic* `sensor_data` de Kafka. Un consumidor con Spark Structured Streaming los lee y calcula el promedio de temperatura y humedad por sensor en ventanas de 1 minuto, mostrando los resultados en consola cada 10 segundos.

### 6.1 Iniciar ZooKeeper y Kafka

```bash
nohup /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties > ~/Tarea3_BigData/kafka/zookeeper.log 2>&1 &
sleep 15
nohup /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties > ~/Tarea3_BigData/kafka/kafka.log 2>&1 &
sleep 20
ss -ltn | grep -E '2181|9092'     # deben aparecer ambos puertos en LISTEN
```

### 6.2 Crear el topic (solo la primera vez)

```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 1 --topic sensor_data
```

### 6.3 Ejecutar productor y consumidor (en dos terminales)

Terminal 1, productor:

```bash
python3 ~/Tarea3_BigData/streaming/kafka_producer.py
```

Terminal 2, consumidor:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
  ~/Tarea3_BigData/streaming/spark_streaming_consumer.py
```

La primera vez, Spark descarga los conectores de Kafka desde Maven y puede tardar varios minutos. Las siguientes ejecuciones arrancan en segundos.

> El sufijo `_2.12` y la versión `3.5.3` del paquete deben coincidir con la versión de Scala y de Spark instaladas.

Mientras el consumidor está activo, la interfaz web de Spark está disponible en `http://<IP-de-la-VM>:4040` (pestañas *Jobs*, *Stages*, *Environment*, *Executors* y *Structured Streaming*).

Para detener cualquiera de los procesos, usa `Ctrl+C`.

## 7. Notas

- El paquete `spark-sql-kafka-0-10_2.12:3.5.3` corresponde a Spark 3.5.3 con Scala 2.12. Con otras versiones de Spark o Scala hay que cambiar las coordenadas.
- `spark.sql.shuffle.partitions` se fija en 4 en el consumidor para que los lotes sean rápidos en una máquina virtual pequeña.
