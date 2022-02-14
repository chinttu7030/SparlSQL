package com.Udemy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}

object MovieRating extends App{

val spark=SparkSession.builder()
  .master("local[*]")
  .appName("Movie Rating")
  .getOrCreate()



  val MovieSchema=new StructType()
    .add("UserId",IntegerType,true)
    .add("MovieId",IntegerType,true)
    .add("rating",IntegerType,true)
    .add("TimeStamp",LongType,true)

  import spark.implicits._
  val data=spark.read
    .option("Inferschema","True")
    .option("Header","True")
    .option("sep","\t")
    .schema(MovieSchema)
    .csv("/Users/devenderreddy/IdeaProjects/SparkSQLExamples/src/main/resources/movie rating")
    .as[Movie]
  data.printSchema()
data.show(false)

}
