package com.dfExamples

import org.apache.spark.sql.functions.array_contains
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DFFilterCondition extends App {

  val spark = SparkSession.builder()
    .master("local[1]").appName("Data Frame filter Multiple Condition")
    .getOrCreate()

  import spark.implicits._

  //Different syntax using filter()

  //  1) filter(condition: Column): Dataset[T]
  //  2) filter(conditionExpr: String): Dataset[T] //using SQL expression
  //  3) filter(func: T => Boolean): Dataset[T]
  //  4) filter(func: FilterFunction[T]): Dataset[T]


  val arrayStructureData = Seq(
    Row(Row("James", "", "Smith"), List("Java", "Scala", "C++"), "OH", "M"),
    Row(Row("Anna", "Rose", ""), List("Spark", "Java", "C++"), "NY", "F"),
    Row(Row("Julia", "", "Williams"), List("CSharp", "VB"), "OH", "F"),
    Row(Row("Maria", "Anne", "Jones"), List("CSharp", "VB"), "NY", "M"),
    Row(Row("Jen", "Mary", "Brown"), List("CSharp", "VB"), "NY", "M"),
    Row(Row("Mike", "Mary", "Williams"), List("Python", "VB"), "OH", "M")
  )

  val arrayStructureSchema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("languages", ArrayType(StringType))
    .add("state", StringType)
    .add("gender", StringType)


  val df = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)
  df.printSchema()
  df.show(false)


  //2)Data Frame filter with column condition

  df.filter(df("state") === "OH").show(false)


  //3)Data Frame filter() with SQL Expression

  df.filter("gender=='M'").show(false)

  //4)Filter with multiple conditions

  df.filter(df("state") === "OH" && df("gender") === "M").show(false)

  //5)Filter on an Array column

  df.filter(array_contains(df("language"), "Java")).show(false)

  //6)Filter on nested struct column

  df.filter(df("name.lastname") === "williams").show(false)


}
