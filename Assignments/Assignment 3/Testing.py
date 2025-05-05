raw_stream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_events") \
    .option("startingOffsets", "latest") \
    .load()

events_df = raw_stream.selectExpr("CAST(value AS STRING)")

query = events_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
