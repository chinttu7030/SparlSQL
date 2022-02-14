package com.dfExamples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.zookeeper.server.SessionTracker.Session

object Selectcolumns extends App {

  val spark = SparkSession.builder
    .master("local[1]")
    .appName("Selecting Columns From DataFrame")
    .getOrCreate()


  val data = Seq(("devender", "reddy", "Hyd", "Telangana"),
    ("Surender", "kudumula", "gudur", "andhra pradesh"),
    ("mounika", "kandanelly", "tandur", "delhi"),
    ("sandhya", "kudumula", "london", "united kingdom"),
    ("Haryaksh", "Reddy", "Miryalaguda", "Telangana"),
    ("Takshvi", "Kudumula", "England", "UK"))

  val columns = Seq("fname", "lname", "place", "dist")

  import spark.implicits._

  val df = data.toDF(columns: _*)
  df.show(false)


  //1)Select Single and Multiple columns

  df.select("fname", "lname").show()

  //Using dataframe object name

  df.select(df("place"), df("dist")).show()


  //2)Select All Columns

  df.select("*").show()
  //  val columnsAll=df.columns.map(m=>col(m))
  //  df.select(columnsAll:_*).show()
  //  df.select(columns.map(m=>col(m)):_*).show()


  //3)Select Columns From list

  val listcol = List("fname", "place")
  df.select(listcol.map(m => col(m)): _*).show()

  //4)Select N columns

  df.select(df.columns.slice(0, 3).map(m => col(m)): _*).show()

  //5)Select column position by index

  df.select(df.columns(3)).show() //selecting particular column number
  df.select(df.columns.slice(2, 4).map(m => col(m)): _*).show() //particular columns

  //6)Select Columns by regular Expression

  //  df.select(df.colRegex("'^.*name*'")).show()

  //7)Select columns starts or end with

  df.select(df.columns.filter(f => f.startsWith("first")).map(m => col(m)): _*)
  df.select(df.columns.filter(f => f.endsWith("lanme")).map(m => col(m)): _*).show()


  //8)Select Nested Columns Struct

  val data2 = Seq(Row(Row("James", "", "Smith"), "OH", "M"),
    Row(Row("Anna", "Rose", ""), "NY", "F"),
    Row(Row("Julia", "", "Williams"), "OH", "F"),
    Row(Row("Maria", "Anne", "Jones"), "NY", "M"),
    Row(Row("Jen", "Mary", "Brown"), "NY", "M"),
    Row(Row("Mike", "Mary", "Williams"), "OH", "M"))

  val schema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("state", StringType)
    .add("gender", StringType)

  val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema)
  df2.printSchema()
  println("df2 data")
  df2.show(false)
  //df2.show(true)

  df2.select("name").show(false)
  df2.select("name.firstname", "name.lastname").show(false)

  df2.select("name.*").show(false)


}
