package com.vamshhi.spark
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


object SparkKafkaIntegraion1 
{
case class Schema(emp_name:String,amount:Int)

def main(args:Array[String]):Unit = {

		val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Kafka Integration").set("spark.driver.allowMultipleContexts","true")
				val sc = new SparkContext(conf)
				sc.setLogLevel("Error")
				val spark = SparkSession.builder().config("spark.cassandra.connection.host","localhost").config("spark.cassandra.connection.port","9042").getOrCreate()
				import spark.implicits._

				//1. Spark Streaming context creation
				val ssc = new StreamingContext(conf,Seconds(1))

				//Kafka Params
				val kafkaParams = Map[String, Object](
						"bootstrap.servers" -> "localhost:9092",
						"key.deserializer" -> classOf[StringDeserializer],
						"value.deserializer" -> classOf[StringDeserializer],
						"group.id" -> "DemoGroup2",
						"auto.offset.reset" -> "latest",
						"enable.auto.commit" -> (true: java.lang.Boolean)
						)

				//2. Array of topics
				val topic = Array("kafkatopic1")

				//3. Kafka Util
				val stream = KafkaUtils.createDirectStream[String, String](
						ssc,
						PreferConsistent,
						Subscribe[String, String](topic, kafkaParams)
						)

				val StreamVal = stream.map(x=>x.value())

				//StreamVal.print()
				StreamVal.foreachRDD(
						x=> if (!x.isEmpty())
						{
							//							val mapSplit = x.map(x=>x.split(",")).map(x=>Schema(x(0),x(1).toInt))
							//									val df = mapSplit.toDF()
							//									df.show()
							//									df.write.format("org.apache.spark.sql.cassandra").option("keyspace","demokeyspace2").option("table","kafka_tab1").mode("append").save()	
							//									println("Data Written to Cassandra")
							val complexJsonDF = spark.read.json(x)
									val explodeDF = complexJsonDF.withColumn("results",explode(col("results"))).select("results.user.username").withColumn("username",regexp_replace(col("username"),"([0-9])",""))
									explodeDF.show()	  
									explodeDF.write.format("org.apache.spark.sql.cassandra").option("keyspace","demokeyspace2").option("table","users_tab").mode("append").save()
									println("Users data written to cassandra.....")
						}
						)
				ssc.start()
				ssc.awaitTermination()
}
}