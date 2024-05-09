from kafka import KafkaConsumer
import csv
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.ml.regression import GBTRegressionModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors, VectorUDT
from pyspark.sql.functions import udf
import psycopg2

db_config = {
    'dbname': 'backendcourse',  # Specify your database name
    'user': 'mysuperuser',  # Use your PostgreSQL username
    'password': 'Vamshi123',  # Use your PostgreSQL password
    'host': 'backendcourse.cpjtimp4gm5c.us-east-1.rds.amazonaws.com',
    'port': 5432  # PostgreSQL default port
}
def connect_to_database():
    try:
        conn = psycopg2.connect(**db_config)
        print("Successfully connected to the database")
        return conn
    except psycopg2.Error as e:
        print("Database connection failed:", e)
        return None


def consume_from_kafka(topic):
    kafka_broker = "localhost:9092"
    consumer = KafkaConsumer(topic, bootstrap_servers=kafka_broker, auto_offset_reset='earliest')
    print(f"Connected to Kafka. Consumer: {consumer}")

    conn = connect_to_database()
    cursor = conn.cursor()
    with open('predicted_data.csv', mode='a', newline='') as file:
        writer = csv.writer(file)
        file.seek(0, 2)  # Move the cursor to the end of the file
        if file.tell() == 0:
            writer.writerow(['DE_temperature', 'DE_radiation_direct_horizontal', 'DE_radiation_diffuse_horizontal', 'DE_solar_generation_prediction'])

        for message in consumer:
            message_data = message.value.decode('utf-8')
            features = [float(i) for i in message_data.split(",")]
            if features[1] == 0 and features[2] == 0:
                up_scale_pred = 0
            else:
                df = spark.createDataFrame([(features,)], ["features"])
                df = df.withColumn("features", array_to_vector_udf(df["features"]))
                # df.show()
                pred = model.transform(df)
                up_scale_pred = 7602.36 + (7204.89 * float(pred.select("prediction").first()[0]))

            # Combine features and prediction to write in a row
            row_to_write = features + [up_scale_pred]
            print(f"Received message from topic {topic}: {row_to_write}")
            sql_insert = """
            INSERT INTO predictions (DE_temperature, DE_radiation_direct_horizontal, DE_radiation_diffuse_horizontal, DE_solar_generation_prediction)
            VALUES (%s, %s, %s, %s)
            """
            cursor.execute(sql_insert, (features[0], features[1], features[2], up_scale_pred))
            conn.commit()
            # Write the data row to CSV
            writer.writerow(row_to_write)
    cursor.close()
    conn.close()
    consumer.close()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("KafkaConsumerApp").getOrCreate()
    array_to_vector_udf = udf(lambda x: Vectors.dense(x), VectorUDT())
    model = GBTRegressionModel.load("final_best_gbt_model")
    kafka_topics = ["weather_final"]

    with ThreadPoolExecutor(max_workers=len(kafka_topics)) as executor:
        executor.map(consume_from_kafka, kafka_topics)
