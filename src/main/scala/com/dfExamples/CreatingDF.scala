package com.dfExamples

import org.apache.spark
import org.apache.spark.sql.SparkSession
import org.apache.zookeeper.server.SessionTracker.Session

object CreatingDF extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("Creating DataFrame")
    .getOrCreate()

  import spark.implicits._

  val columns = Seq(("Language"), ("User-Count"))
  val data = Seq(("java", "2000"), ("Python", "3000"), ("Scala", "5000"))

  //1)Creating DataFrame from RDD


  val rdd = spark.sparkContext.parallelize(data)

  //1.1)Using to DF()Function

  val dfFromRDD1 = rdd.toDF()
  dfFromRDD1.printSchema()

  val dfFromRDD2 = rdd.toDF("Language", "User-Count")
  dfFromRDD2.printSchema()

  //1.2)Spark DataFrame with Spark Session

  // val dfFromRDD3=spark.createDataFrame(rdd).toDF(columns:_*)

  //1.3)Create DataFrame Using RowType


  //READ CSV file into DataFrame

  val df = spark.read.csv("/Users/devenderreddy/IdeaProjects/SparkSQLExamples/src/main/resources/zipcodes.csv")
  df.printSchema()

  val df1 = spark.read.option("header", true).csv("/Users/devenderreddy/IdeaProjects/SparkSQLExamples/src/main/resources/zipcodes.csv")
  df1.printSchema()


}
