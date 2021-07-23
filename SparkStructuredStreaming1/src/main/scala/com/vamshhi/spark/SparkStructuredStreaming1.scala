package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming._

object SparkStructuredStreaming1 {

	def main(args:Array[String]):Unit = {

			val conf = new SparkConf().setAppName("Spark Structured Streaming").setMaster("local[*]")
					.set("spark.driver.allowMultipleContexts","true")
					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")
					val spark = SparkSession.builder().getOrCreate()

					import spark.implicits._

					//val ssc = new StreamingContext(conf,Seconds(2))
					val weblogschema = StructType(Array(
							StructField("Streamdata",StringType,true)
							))
					val Df = spark.readStream.schema(weblogschema).option("sep","~").csv("file:///C:/Data/NifiRandomUserUrl")
					.writeStream.outputMode("append").format("parquet").option("checkpointLocation","file:///C:/checkpointDir")
					.option("truncate","false").start("file:///C:/Data/NifiRandomUserUrl/Spark_parquetWrite").awaitTermination()
	}
}