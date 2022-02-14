import sbt._

object Dependencies {

  lazy val spark = {
    Seq(
      ("org.apache.hadoop" % "hadoop-aws" % "2.8.5")
        .exclude("com.fasterxml.jackson.core", "jackson-annotations"),
      "com.amazonaws" % "aws-java-sdk-s3" % "1.10.77",
      "com.amazonaws" % "aws-java-sdk-sts" % "1.10.77",
      "org.apache.spark" %% "spark-sql" % SparkVersion,
      "org.apache.spark" %% "spark-core" % SparkVersion,
      "org.apache.spark" %% "spark-avro" % SparkVersion
    ).map { dependency =>
      dependency
        .exclude("asm", "asm")
        .exclude("javax.ws.rs", "javax.ws.rs-api")
    }
  }
  
  lazy val scalatest = "org.scalatest" %% "scalatest" % ScalatestVersion % "test"
  
  lazy val all = spark ++ Seq(scalatest)

  lazy val JacksonScalaVersion = "2.9.8"
  lazy val ScalatestVersion = "3.0.7"
  lazy val SparkVersion = "2.4.3"
}
