package com.dfExamples

import com.dfExamples.Selectcolumns.df2
import org.apache.spark
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions.col

object ADDINGCOLUMN extends App {


  val spark: SparkSession = SparkSession.builder()
    .master("local[1]").appName("Creating DataFrame")
    .getOrCreate()

  import spark.implicits._


  val data = Seq(Row(Row("James;", "", "Smith"), "36636", "M", "3000"),
    Row(Row("Michael", "Rose", ""), "40288", "M", "4000"),
    Row(Row("Robert", "", "Williams"), "42114", "M", "4000"),
    Row(Row("Maria", "Anne", "Jones"), "39192", "F", "4000"),
    Row(Row("Jen", "Mary", "Brown"), "", "F", "-1"))

  val schema = new StructType()
    .add("name", new StructType()
      .add("firstname", StringType)
      .add("middlename", StringType)
      .add("lastname", StringType))
    .add("dob", StringType)
    .add("gender", StringType)
    .add("salary", StringType)


  val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  df.printSchema()
  df.show(true)
  df.show(false)
  println("Operations started Executed")


  //1)Add new column to data frame
  df.withColumn("country", lit("India")).show()
  //Lit() function used to add a constant value to a data frame column


  //2)Change the value of an existing column

  df.withColumn("salary", col("salary") + 100).show()

  //3)Derive New column from an Existing column

  df.withColumn("copied column", col("salary") * -1).show()

  //4)change column data type

  df.withColumn("salary", col("salary").cast("Integer")).show()

  //5)Add, Replace, or update multiple columns

  df2.createOrReplaceTempView("Person")
  spark.sql("SELECT salary *100 as salary, salary*-1 as copied colomn,'INDIA' as country FROM person").show()

  //6)Rename column name

  df.withColumnRenamed("gender", "sex").show()

  //7)Drop a column

  df.drop("copied column")

  //8)Split column into multiple columns

  //  val columns = Seq("name","address")
  //  val data = Seq(("Robert, Smith", "1 Main st, Newark, NJ, 92537"),
  //    ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732"))
  //  var dfFromData = spark.createDataFrame(data).toDF(columns:_*)
  //  dfFromData.printSchema()
  //
  //  val newDF = dfFromData.map(f=>{
  //    val nameSplit = f.getAs[String](0).split(",")
  //    val addSplit = f.getAs[String](1).split(",")
  //    (nameSplit(0),nameSplit(1),addSplit(0),addSplit(1),addSplit(2),addSplit(3))
  //  })
  //  val finalDF = newDF.toDF("First Name","Last Name",
  //    "Address Line1","City","State","zipCode")
  //  finalDF.printSchema()
  //  finalDF.show(false)

}

