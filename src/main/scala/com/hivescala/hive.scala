package com.hivescala


import org.apache.spark.sql.SparkSession

object hive {
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").enableHiveSupport().appName("Hive Scala").getOrCreate()
    val df = spark.read.format("csv").option("header","true").load("Sample100.csv")
//    df.printSchema()
    spark.sql("drop table if exists employee")
    df.createOrReplaceTempView("employee")
    spark.sql("create table employee as select * from employee")
    val dfhive = spark.sql("select * from employee")
    dfhive.printSchema()
  }
}
