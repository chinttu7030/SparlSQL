package com.Udemy

import org.apache.spark.sql.SparkSession

object Singaporeagegroup extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Crop Data")
    .getOrCreate()

  import spark.implicits._

  val crops = spark.read
    .option("Inferschema", "True")
    .option("Header", "True")
    .option("sep", ",")
    .csv("/Users/devenderreddy/IdeaProjects/SparkSQLExamples/src/main/resources/singapore-residents-by-age-group-ethnic-group-and-sex-end-june-annual.csv")
    .as[Sagegroup]

  

  crops.printSchema()
  crops.show(false)





}
