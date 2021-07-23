package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._

object SparkCassandraTask3 {
  
  def main(args:Array[String]):Unit = {
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Cassandra 3")                        
    val sc = new SparkContext(conf)
    sc.setLogLevel("Error")
    val cassandra_tab_Rdd = sc.cassandraTable("demokeyspace1","txns_records")
    cassandra_tab_Rdd.take(10).foreach(println)
  }
}