package com.vamshhi.spark

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SQLContext, _}
import org.apache.spark.sql.execution.datasources.hbase._
import org.apache.spark.{SparkConf, SparkContext}

// Spark Project 3 Includes -> MYSQL(RDBMS),HBASE,SPARK,HIVE

object Spark_Project2 
{
	def main(args:Array[String]):Unit = 
		{
				val spark = SparkSession.builder().appName("Spark Project2").master("local[*]").enableHiveSupport().getOrCreate()
				val sc = spark.sparkContext
				sc.setLogLevel("Error")
				import spark.implicits._
				val schema_df = spark.read.format("jdbc")
				                     .option("url","jdbc:mysql://192.168.56.101:3306/Schema_DB")
										         .option("driver","com.mysql.jdbc.Driver")
										         .option("dbtable","Schema_tab")
										         .option("user","root")
										         .option("password","cloudera").load()
				
				//val schema_df = spark.read.format("csv").option("header","true").load("file:///C:/Data/Spark_project3_source/schema.txt")
				schema_df.show() 

				val schema_list = schema_df.select("*").collect().toList
				//print(schema_list)

				val str1 = 
				s""" 
				|{
				|"table" : { "namespace" : "default", "name" : "hbase_tract12" },
				|"rowkey" : "rowkey",
				|"columns" :
				|{
				|"master_id" : { "cf" : "rowkey", "col" : "rowkey","type" : "string" },
				""".stripMargin+"\n"
						
				//dynamic HBase catalog string creation
				val str2 = StringBuilder.newBuilder
				for(schema <- schema_list) 
				{
					val scolumn = schema.getString(1)
					val hcolumn = schema.getString(0)
					val str = s""" "$scolumn" : { "cf" : "cf" ,"col" : "$hcolumn","type" : "string" },""" + "\n"
					str2++= str
				}
				  val str3 = "}"+"\n"+"}"
					val catalog = str1+str2.toString.replaceAll(",$", "")+str3
					//println(catalog)
					
					val hbase_track_df = spark.read.options(Map(HBaseTableCatalog.tableCatalog->catalog)).format("org.apache.spark.sql.execution.datasources.hbase").load()

					val hive_table_names = schema_list.map(x=>x.getString(1)).map(x=>x.split("_")(0)).distinct
					print(hive_table_names)
				  
				  val stmt1 = spark.sql("DROP DATABASE IF EXISTS hive_tract12_db CASCADE")
				  val stmt2 = spark.sql("CREATE DATABASE hive_tract12_db LOCATION '/user/hive/warehouse'")
				  
				  for(table_name <- hive_table_names) 
				  {
				    val col_arr = hbase_track_df.columns.filter(_.startsWith(s"$table_name"))
				    val col_arr1 = Array("master_id") ++ col_arr 
            val res_df = hbase_track_df.select(col_arr1.map(col):_*)	   
				    res_df.write.format("hive").mode("append").saveAsTable("hive_tract12_db."+table_name.concat("_tab"))    
				  }
		} 
}