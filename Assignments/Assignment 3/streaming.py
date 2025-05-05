from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, count, avg, expr, when, sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import to_json, struct


spark = SparkSession.builder.appName("traffic_events").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_events") \
    .option("startingOffsets", "latest") \
    .load()


schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("vehicle_count", IntegerType(), True),
    StructField("average_speed", DoubleType(), True),
    StructField("congestion_level", StringType(), True)
])


json_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data"))
events_df = parsed_df.select("data.*")


events_df = events_df.withColumn("event_time", (col("timestamp") * 1000).cast(TimestampType()))


events_df = events_df \
    .filter(col("sensor_id").isNotNull() & col("timestamp").isNotNull()) \
    .filter((col("vehicle_count") >= 0) & (col("average_speed") > 0)) \
    .dropDuplicates(["sensor_id", "timestamp"])


traffic_volume = events_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "5 minutes"), col("sensor_id")) \
    .agg(count("vehicle_count").alias("vehicle_count"))


congestion_hotspots = events_df \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(window(col("event_time"), "5 minutes"), col("sensor_id")) \
    .agg(count(when(col("congestion_level") == "HIGH", 1)).alias("high_count")) \
    .filter(col("high_count") >= 3)


avg_speed = events_df \
    .withWatermark("event_time", "15 minutes") \
    .groupBy(window(col("event_time"), "10 minutes"), col("sensor_id")) \
    .agg(avg("average_speed").alias("average_speed"))


speed_drop = events_df.alias("curr") \
    .join(events_df.alias("prev"), \
          (col("curr.sensor_id") == col("prev.sensor_id")) &
          (col("curr.event_time") - col("prev.event_time") <= expr("interval 2 minutes")), "inner") \
    .filter((col("prev.average_speed") > 0) &
            ((col("prev.average_speed") - col("curr.average_speed")) / col("prev.average_speed") > 0.5)) \
    .select("curr.sensor_id", "curr.event_time", "curr.average_speed", "prev.average_speed")


busiest_sensors = events_df \
    .withWatermark("event_time", "30 minutes") \
    .groupBy(window(col("event_time"), "30 minutes"), col("sensor_id")) \
    .agg(sum("vehicle_count").alias("total_vehicles"))


def send_to_kafka(df, topic):
    df.selectExpr("CAST(sensor_id AS STRING) AS key", "to_json(struct(*)) AS value") \
      .writeStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("topic", topic) \
      .option("checkpointLocation", "/tmp/checkpoints/" + topic) \
      .start()

send_to_kafka(traffic_volume, "traffic_analysis")
send_to_kafka(avg_speed, "traffic_analysis")
send_to_kafka(congestion_hotspots, "traffic_analysis")
send_to_kafka(busiest_sensors, "traffic_analysis")
send_to_kafka(speed_drop, "traffic_analysis")


query1 = traffic_volume.writeStream.outputMode("append").format("console").start()
query2 = congestion_hotspots.writeStream.outputMode("append").format("console").start()
query3 = avg_speed.writeStream.outputMode("append").format("console").start()
query4 = speed_drop.writeStream.outputMode("append").format("console").start()
query5 = busiest_sensors.writeStream.outputMode("complete").format("console").start()

query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
query4.awaitTermination()
query5.awaitTermination()
