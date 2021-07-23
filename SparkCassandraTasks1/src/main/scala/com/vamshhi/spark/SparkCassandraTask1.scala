package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SparkCassandraTask1 {
	def main(args:Array[String]):Unit = {

			val spark = SparkSession.builder().appName("Spark Cassandra").master("local[*]")
			                        .config("spark.cassandra.connection.host","localhost")
			                        .config("spark.cassandra.connection.port","9042").getOrCreate()
					val sc = spark.sparkContext
					sc.setLogLevel("Error")
					
					//reading cassandra table : txns_records
					val txns_cassandra_df = spark.read.format("org.apache.spark.sql.cassandra")
					                             .option("keyspace","demokeyspace1")
					                             .option("table","txns_records").load()
					txns_cassandra_df.show(false)
					
					//writing to cassandra table : txns_records_write
          txns_cassandra_df.write.format("org.apache.spark.sql.cassandra")
                           .option("keyspace","demokeyspace1")
                           .option("table","txns_records_write").mode("append").save()
          print("========Data written to Cassandra table========")
	}
}