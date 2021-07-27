package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

object SparkStreaming_Kafka 
{
	def main(args:Array[String]):Unit = {

			val conf = new SparkConf().setAppName("Spark Streaming - Kafka").setMaster("local[*]").set("spark.driver.allowMultipleContexts","true")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()

					//1. Spark Streaming context creation
					val ssc = new StreamingContext(conf,Seconds(1))

					//2. Kafka Params
					val kafkaParams = Map[String, Object](
							"bootstrap.servers" -> "localhost:9092",
							"key.deserializer" -> classOf[StringDeserializer],
							"value.deserializer" -> classOf[StringDeserializer],
							"group.id" -> "DemoGrp1",
							"auto.offset.reset" -> "latest",
							"enable.auto.commit" -> (true: java.lang.Boolean)
							)

					//3. Array of topics
					val topic =  Array("demoktopic1")

					//4. Kafka Utils
					val stream = KafkaUtils.createDirectStream[String, String](
							ssc,PreferConsistent,
							Subscribe[String, String](topic, kafkaParams))

					//5. printing stream value
					val streamValue = stream.map(x=>x.value())
					streamValue.print()

					ssc.start()
					ssc.awaitTermination()
	}
}