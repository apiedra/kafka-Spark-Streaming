package org.example

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Minutes, StreamingContext}


object App {

  var sparkSession: SparkSession = _

  def main(args: Array[String]): Unit = {

    AppProperties.instance().initProperties("")

    val topics = Array(AppProperties.instance().KafkaProperties.topicName)


    val sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Service , Local")
      .config("spark.driver.host", "localhost")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.exec.max.dynamic.partitions", "3000")
      .config("spark.streaming.backpressure.enabled", "true")
      .config("spark.streaming.backpressure.initialRate", "100")
      .config("spark.streaming.kafka.maxRatePerPartition", "100")
      .config("spark.streaming.receiver.maxRate", "100")
      .config("spark.streaming.stopGracefullyOnShutdown", "true")
      .enableHiveSupport()
      .getOrCreate()

    val ssc = new StreamingContext(sparkSession.sparkContext, Minutes(1))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    stream.foreachRDD(rdd => {
      import sparkSession.implicits._

      sparkSession.read.json(sparkSession.createDataset(rdd.map(_.value()))).select("%s.*" format (AppProperties.instance().KafkaProperties.topicRoot)).show(false)
    })
    ssc.start()
    ssc.awaitTerminationOrTimeout(TimeUnit.MINUTES.toMillis(15))
    ssc.stop()
  }

  private def kafkaParams: Map[String, Object] = {
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> AppProperties.instance().KafkaProperties.bootstrapServers,
      "group.id" -> AppProperties.instance().KafkaProperties.topicName,
      "key.deserializer" -> AppProperties.instance().KafkaProperties.keyDeserializer,
      "value.deserializer" -> AppProperties.instance().KafkaProperties.valueDeserializer,
      "auto.commit.interval.ms" -> AppProperties.instance().KafkaProperties.autoCommitInterval,
      "session.timeout.ms" -> AppProperties.instance().KafkaProperties.sessionTimeout,
      "auto.offset.reset" -> AppProperties.instance().KafkaProperties.autoOffset,
      "enable.auto.commit" -> (AppProperties.instance().KafkaProperties.enableAutoCommit: java.lang.Boolean)
    )
    kafkaParams
  }


}
