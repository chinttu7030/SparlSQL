package com.Udemy

import javafx.beans.binding.Bindings.select
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.SELECT
import org.apache.spark.sql.functions.{avg, col, round}
import org.apache.spark.sql.{Dataset, SparkSession}

object ZipCodes extends App {

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("Udemy Examples")
    .getOrCreate()

  import spark.implicits._

  var zipcodes: Dataset[codes] = spark.read
    .option("Header", "True")
    .option("Inferschema", "True")
    .csv("/Users/devenderreddy/IdeaProjects/SparkSQLExamples/src/main/resources/zipcodes.csv")
    .as[codes]

  // zipcodes.select(col(codes.apply(RecordNumber = ???, Zipcode = ???, City = ???, State = ???)))
  //  zipcodes = zipcodes.select(col("RecordNumber"),
  //    col("Zipcode"),
  //    col("City"),
  //    col(("State"))).as[codes]
  zipcodes.show(40, false)
  zipcodes.printSchema()

  zipcodes.select(col("RecordNumber")).show()

  zipcodes.filter(zipcodes("RecordNumber") < 10).show()
  zipcodes.groupBy("State").count().show()
  zipcodes.select(col("City"), col("Zipcode") + 10).show()
  //zipcodes.select(col("ZipCodeType"=="STANDARD")).show()
  zipcodes.filter(zipcodes("ZipCodeType") === "STANDARD").show()
  val ds = zipcodes.select("Zipcode", "City")
  ds.show()
  ds.groupBy("Zipcode").agg(round(avg("Zipcode"))).sort("Zipcode").show()
  zipcodes.sort("Zipcode")


}
