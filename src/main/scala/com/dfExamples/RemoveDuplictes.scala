package com.dfExamples

import org.apache.spark.sql.SparkSession

object RemoveDuplictes extends App {


  val spark = SparkSession.builder()
    .master("local[1]").appName("To Remove Duplicates")
    .getOrCreate()


  import spark.implicits._



  val simpleData = Seq(("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
  )
  val df = simpleData.toDF("employee_name", "department", "salary")
  df.show()
  df.printSchema()

  //1)Use distinct() – Remove Duplicate Rows on DataFrame


  val distinctDF = df.distinct()
  //distinctDF.sh
  println("Distinct count: " + distinctDF.count())
  distinctDF.show()


}
