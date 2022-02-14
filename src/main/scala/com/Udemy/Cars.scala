package com.Udemy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col


object Cars extends App{

  val spark=SparkSession.builder()
    .master("local[*]")
    .appName("Cars Example")
    .getOrCreate()

  import spark.implicits._

  val cars=spark.read.option("Header","True")
    .option("Inferschema","True")
    .csv("/Users/devenderreddy/IdeaProjects/SparkSQLExamples/src/main/resources/cars.csv")
    .as[carsexample]
  cars.printSchema()
  cars.show()

//  cars.select("Car","Cylinders").show()
//  cars.select("*").show()
//  cars.select(cars.columns(3)).show()
//  cars.withColumn("copied column",col("Cylinders") * 10).show()
//  cars.drop("copied column")

  //Filter Operations

//  cars.filter(cars("Displacement")<300).show()//Data frame with column condition
//  cars.filter(cars("Cylinders")===8).show()
//
  cars.filter("Origin=='US'").show(false)//Data Frame with SQL
  cars.where("Origin=='US'").show(false)
//  cars.filter(cars("MPG")===18 && cars("Model")===70).show()//Multiple filter in column









}
