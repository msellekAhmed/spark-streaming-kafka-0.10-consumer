import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object TestKafkaConsumerApp{
  def main(args: Array[String]) {

    val conf = new SparkConf().setMaster("local[2]").setAppName("TestKafkaConsumer")
    lazy val streamingContext = new StreamingContext(conf, Seconds(3))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "example",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("test")
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val ds = stream.map(record => (record.key, record.value))

    stream.foreachRDD { rdd =>
      val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      if (!rdd.isEmpty()) {
        println("!!!! It works3 : " + rdd)
        rdd.saveAsTextFile("/Users/irybitskyi.ctr/projects/ds/test-consumer/test-data/" + System.currentTimeMillis().toString)
      }

      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
    }

    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
