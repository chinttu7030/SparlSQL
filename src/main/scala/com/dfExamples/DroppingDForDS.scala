package com.dfExamples

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DroppingDForDS extends App {

  val spark = SparkSession.builder()
    .master("local[1]").appName("Dropping Data Frame or Data set Columns")
    .getOrCreate()

  import spark.implicits._


  val structureData = Seq(
    Row("James", "", "Smith", "36636", "NewYork", 3100),
    Row("Michael", "Rose", "", "40288", "California", 4300),
    Row("Robert", "", "Williams", "42114", "Florida", 1400),
    Row("Maria", "Anne", "Jones", "39192", "Florida", 5500),
    Row("Jen", "Mary", "Brown", "34561", "NewYork", 3000)
  )

  val structureSchema = new StructType()
    .add("firstname", StringType)
    .add("middlename", StringType)
    .add("lastname", StringType)
    .add("id", StringType)
    .add("location", StringType)
    .add("salary", IntegerType)

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(structureData), structureSchema)
  df.printSchema()
  df.show()


  //1) Drop one column from data frame

  df.drop(col("firstname")).printSchema()
  //df.drop(df("firstname")).printSchema()


  //2)Drop multiple columns from data frame


  df.drop("firstname", "lastname", "middle name").printSchema()


   val cols=Seq("firstname","last name","middle name")
  df.drop(cols:_*).printSchema()


}
