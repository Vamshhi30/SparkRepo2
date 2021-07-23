package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Spark_Project2 
{
	def main(args:Array[String]):Unit = 
		{
				val spark = SparkSession.builder().appName("Spark Project2").master("local[*]").getOrCreate()
				val sc = spark.sparkContext
				sc.setLogLevel("Error")
				val schema_df = spark.read.format("csv").option("header","true").load("file:///C:/Data/Spark_project3_source/schema.txt")
				schema_df.show() 

				val schema_list = schema_df.select("*").collect().toList
				//print(schema_list)

				val str1 = 
				s""" 
				|{
				|"table" : { "namespace" : "default", "name" : "hbase_tract11"},
				|"rowkey" : "rowkey",
				|"columns" :
				|{
				|"master_id" : { "cf" : "rowkey", "col" : "rowkey","type" : "string"},
				""".stripMargin+"\n"
						
				//dynamic HBase catalog string creation
				val str2 = StringBuilder.newBuilder
				for(schema <- schema_list) 
				{
					val scolumn = schema.getString(1)
					val hcolumn = schema.getString(0)
					val str = s""" "$scolumn" : { "cf" : "cf" ,"col" : "$hcolumn","type" : "string"},""" + "\n"
					str2++= str
				}
				  val str3 = "}"
					val catalog = str1+str2.toString.replaceAll(",$", "")+str3
					println(catalog)
		} 
}