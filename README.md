# Architecture

- Run local kafka to produce a message every second
- Run local Spark
- Consume messages from kafka and println it

## Spark Kafka Consumer - Direct Approach
https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/DirectKafkaWordCount.scala


# References

http://spark.apache.org/docs/latest/streaming-kafka-integration.html 

https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/KafkaWordCount.scala
