package com.Udemy

import org.apache.parquet.format.IntType
import org.apache.spark.sql.types.{StringType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Textfile extends App {


  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Text File")
    .getOrCreate()

  import spark.implicits._

  val customer: DataFrame = spark.read
    .option("Inferschema", "True")
    //.option("Header","True")
    .option("sep", "|")
    .option("Schema", "RowNumber:Int,Age:Int,Gender:Int,Occupation:String,Count:Int")
    .csv("/Users/devenderreddy/IdeaProjects/SparkSQLExamples/src/main/resources/u.user")

  //   val schema=Structype(Array(
  //     StructField("RowNumber",IntType,true)
  //     StructField("Age",IntType,true)
  //     StructField("Gender",IntType,true)
  //     StructField("Occupation",StringType,true)
  //     StructField("Count",IntType,true)
  //   ))
  customer.printSchema()
  customer.show(false)

}


