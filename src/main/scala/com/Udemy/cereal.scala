package com.Udemy

import org.apache.spark.sql.SparkSession

object cereal extends App{


  val spark=SparkSession.builder()
    .master("local[*]")
    .appName("Cereal")
    .getOrCreate()

  import spark.implicits._

  val cereals=spark.read.format("CSV").option("Inferschema","True")
    .option("Header","True").option("sep", ";")
    .load("/Users/devenderreddy/IdeaProjects/SparkSQLExamples/src/main/resources/cereal.csv")


  cereals.printSchema()
  cereals.show(numRows = 100)
}
