package com.dfExamples

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Row, SparkSession}


object RenameNestedColumn extends App {


  val spark = SparkSession.builder()
    .master("local[1]").appName("Rename Nested Column")
    .getOrCreate()




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
    .add("dob", StringType)
    .add("gender", StringType)
    .add("salary", IntegerType)


  val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  df.printSchema()
  df.show(false)


  //1) To rename data frame column name

  df.withColumnRenamed("dob", "Date of Birth")


  //2) To rename nested column in data frame


  val schema2 = new StructType()
    .add("fname", StringType)
    .add("middlename", StringType)
    .add("lname", StringType)


  df.select(col("name").cast(schema2),
    col("dob"),
    col("gender"),
    col("salary"))
    df.printSchema()


  //3) To change all columns in a spark data frame

  val newcolumns = Seq("newcol1", "newcol2", "newcol3")
  val df3 = df.toDF(newcolumns: _*)
  df3.printSchema()
  df3.show(false)


}
