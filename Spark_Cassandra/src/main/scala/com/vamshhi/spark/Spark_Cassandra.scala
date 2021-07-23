package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Spark_Cassandra {
	def main(args:Array[String]):Unit = {

			val spark = SparkSession.builder().appName("Spark Cassandra").master("local[*]").config("spark.cassandra.connection.host","localhost").config("spark.cassandra.connection.port","9042").getOrCreate()
					val sc = spark.sparkContext
					sc.setLogLevel("Error")

					val txns_df = spark.read.format("org.apache.spark.sql.cassandra").option("keyspace","demokeyspace2").option("table","txns_tab").load()
					txns_df.show(false)
					txns_df.coalesce(1).write.format("com.databricks.spark.xml").option("rootTag","transactions").option("rowTag","transaction").mode("overwrite").save("file:///D:/D Data/ResultDir/cassandra_xml")
					print("==========================Data Written===================")
	}
}