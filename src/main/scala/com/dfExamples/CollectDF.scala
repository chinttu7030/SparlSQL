package com.dfExamples

import org.apache.spark.sql.catalyst.ScalaReflection.Schema
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object CollectDF extends App {

  val spark = SparkSession.builder()
    .master("local[1]").appName("Collect- Retrieve data from spark DF")
    .getOrCreate()

  import spark.implicits._

  val data = Seq(Row(Row("James ", "", "Smith"), "36636", "M", 3000),
    Row(Row("Michael ", "Rose", ""), "40288", "M", 4000),
    Row(Row("Robert ", "", "Williams"), "42114", "M", 4000),
    Row(Row("Maria ", "Anne", "Jones"), "39192", "F", 4000),
    Row(Row("Jen", "Mary", "Brown"), "", "F", -1)
  )

  val schema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("id", StringType)
    .add("gender", StringType)
    .add("salary", IntegerType)

  val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  df.printSchema()
  df.show(false)

  //  collect() action function is used to retrieve all elements from the dataset (RDD/DataFrame/Dataset) as a Array[Row] to the driver program.

  //  collectAsList() action function is similar to collect() but it returns Java util list.

  //syntax:  collect():scala.Array[T]
  //          collectAsList: java.util.List[T]


  //1) Collect example

  val colList = df.collectAsList()



//  //Retrieving data from Struct column
//  colData.foreach(row => {
//    val salary = row.getInt(3)
//    val fullName: Row = row.getStruct(0) //Index starts from zero
//    val firstName = fullName.getString(0) //In struct row, again index starts from zero
//    val middleName = fullName.get(1).toString
//    val lastName = fullName.getAs[String]("lastname")
//    println(firstName + "," + middleName + "," + lastName + "," + salary)
//  )


}
