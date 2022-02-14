package com.Udemy

import com.dfExamples.CreatingDF.{columns, df1}
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.Stream.Empty.print

object DataBricks extends App {

  val spark=SparkSession.builder()
    .master("[local[1]]")
    .appName("DataBricks")
    .getOrCreate()

  import spark.implicits._

  val data=Seq(("1","station 1","4:20 AM"),("2","station 2","5:30 AM"),("3","station 3","7:30 AM"),
    ("2","station 2","5:50 AM"),("2","station 2","7:30 AM"),("2","station 2","11:30 AM"),("2","station 2","1:30 AM"))
  val schema=Seq(("BUS ID","STATION","TIME"))


  val df = data.toDF(columns:_*)
  df.show()










}
