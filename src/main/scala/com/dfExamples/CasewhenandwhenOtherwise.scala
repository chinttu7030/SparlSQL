package com.dfExamples

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, when}

object CasewhenandwhenOtherwise extends App {

  val spark = SparkSession.builder()
    .master("local[1]").appName("Case when and when Otherwise")
    .getOrCreate()

  import spark.implicits._

  val data = List(("James", "", "Smith", "36636", "M", 60000),
    ("Michael", "Rose", "", "40288", "M", 70000),
    ("Robert", "", "Williams", "42114", "", 400000),
    ("Maria", "Anne", "Jones", "39192", "F", 500000),
    ("Jen", "Mary", "Brown", "", "F", 0))

  val cols = Seq("first_name", "middle_name", "last_name", "dob", "gender", "salary")

  val df = spark.createDataFrame(data).toDF(cols: _*)
  df.printSchema()
  df.show(false)


  //1)Using "when otherwise" on spark dataframe

  val df2 = df.withColumn("new_gender", when(col("gender") === 'M', "MALE").when(col("gender") === "F", "FEMALE").otherwise("UNKNOWN"))
  df2.show()


  val df4 = df.select(col("*"), when(col("gender") === "M", "Male")
    .when(col("gender") === "F", "Female")
    .otherwise("Unknown").alias("new_gender"))
  df4.show()

  //2)Using Case when on spark data frame

  val df3 = df.withColumn("new_gender", expr("case when gender='M' " +
    "when gender='F' thenn 'FEMALE'" + "else 'Unknown' end"))
  df3.show()

  //USING SQL SELECT
  val df5 = df.select(col("*"), expr("case when gender='M' then 'MALE'" +
    "when gender 'F' then 'FEMALE' " + "else 'UNKNOWN' end").alias("new_gender"))
  df5.show()


  //3)Using && and || operator


  val dataDF = Seq(
    (66, "a", "4"), (67, "a", "0"), (70, "b", "4"), (71, "d", "4"
    )).toDF("id", "code", "amt")
  dataDF.withColumn("new_column",
    when(col("code") === "a" || col("code") === "d", "A")
      .when(col("code") === "b" && col("amt") === "4", "B")
      .otherwise("A1"))
    .show()


}
