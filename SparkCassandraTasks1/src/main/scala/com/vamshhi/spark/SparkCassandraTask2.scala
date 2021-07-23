package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkCassandraTask2 {

	def main(args:Array[String]):Unit = {

			val spark = SparkSession.builder().appName("Spark Cassandra 2").master("local[*]")
					.config("spark.cassandra.connection.host","localhost")
					.config("spark.cassandra.connection.port","9042")
					.getOrCreate()

					val sc = spark.sparkContext
					sc.setLogLevel("Error")
					val txns_records_df = spark.read.format("org.apache.spark.sql.cassandra")
					                      .option("keyspace","demokeyspace1")
					                      .option("table","txns_records")
					                      .load()
					val txns_records_write_df = spark.read.format("org.apache.spark.sql.cassandra")
					                      .option("keyspace","demokeyspace1")
					                      .option("table","txns_records_write")
					                      .load()
					txns_records_df.show(false)
					txns_records_write_df.show(false)
					
					val union_df = txns_records_df.union(txns_records_write_df)
					union_df.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite")
					        .save("file:///D:/D Data/ResultDir/cassandra_resDir")
					print("========Data Written=========")
}}