package com.Udemy

import com.dfExamples.Selectcolumns.df
import org.apache.spark.metrics.source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, StructField, StructType}
import org.json4s.scalap.scalasig.ClassFileParser.fields

import scala.io.Source
import scala.reflect.internal.util.NoFile.file

object MovieNames extends App {



  //  def LoadmovieNames():Map[Int,String]={
  //    var movienames:Map[Int,String]=Map()
  //    val Lines=Source.fromFile("/Users/devenderreddy/IdeaProjects/SparkSQLExamples/src/main/resources/u.item")
  //    for(line<-Lines.getLines()){
  //      val fields=line.split('|')
  //      if(fields.length>1){
  //        movienames =(fields(0) toInt-> fields(1))
  //      }
  //    }
  //    Lines.close()
  //    movienames
  //  }

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Movie Names")
    .getOrCreate()


  //val dict=spark.sparkContext.broadcast(LoadmovieNames())

  val schema = new StructType()
    .add("MovieId", IntegerType, true)
    .add("MovieName(Year)", StringType, true)
    .add("releaseDate", StringType, true)
    .add("test", StringType, true)
    .add("Website", StringType, true)
    .add("a", IntegerType, true)
    .add("b", IntegerType, true)
    .add("c", IntegerType, true)
    .add("d", IntegerType, true)
    .add("e", IntegerType, true)
    .add("f", IntegerType, true)
    .add("g", IntegerType, true)
    .add("h", IntegerType, true)
    .add("i", IntegerType, true)
    .add("j", IntegerType, true)
    .add("k", IntegerType, true)
    .add("l", IntegerType, true)
    .add("m", IntegerType, true)
    .add("n", IntegerType, true)
    .add("o", IntegerType, true)
    .add("p", IntegerType, true)
    .add("q", IntegerType, true)
    .add("r", IntegerType, true)
    .add("s", IntegerType, true)


  import spark.implicits._

  val movienames = spark.read
    //.option("Inferschema","True")
    //.option("Header","True")
    .option("sep", "|")
    .schema(schema)
    .csv("/Users/devenderreddy/IdeaProjects/SparkSQLExamples/src/main/resources/u.item")

  movienames.printSchema()
  movienames.show(false)

  val Names = movienames.select(movienames("MovieId"), movienames("MovieName(Year)"), movienames("releaseDate")).show(false)

  val Moviecounts = movienames.groupBy("MovieId").count().show(20)


}
